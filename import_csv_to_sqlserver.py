"""
Script para importar arquivos CSV para SQL Server
Importa todos os CSVs modificados hoje para o banco 'Olist Brazilian E-Commerce'
"""

import os
import pandas as pd
import pyodbc
from datetime import datetime
from pathlib import Path
import sys

# Configurações de conexão SQL Server
SERVER = 'localhost'  # Altere se necessário
DATABASE = 'Olist Brazilian E-Commerce'
DRIVER = '{ODBC Driver 17 for SQL Server}'  # Ajuste conforme seu driver instalado

# Diretório onde estão os CSVs
CSV_DIR = r'C:\Users\lopes\Downloads'

def get_connection_string():
    """Retorna a string de conexão para SQL Server"""
    return f"""
    DRIVER={DRIVER};
    SERVER={SERVER};
    DATABASE={DATABASE};
    Trusted_Connection=yes;
    """

def get_csvs_modified_today(directory):
    """Retorna lista de arquivos CSV modificados hoje"""
    today = datetime.now().date()
    csv_files = []
    
    for file in Path(directory).glob('*.csv'):
        file_date = datetime.fromtimestamp(file.stat().st_mtime).date()
        if file_date == today:
            csv_files.append(file)
            print(f"[OK] Encontrado CSV modificado hoje: {file.name}")
    
    return csv_files

def infer_sql_type(pandas_type, sample_values=None):
    """Infere o tipo SQL apropriado baseado no tipo do pandas"""
    if pandas_type == 'object':
        # Verifica se é data tentando converter alguns valores
        if sample_values is not None and len(sample_values) > 0:
            # Tenta converter os primeiros valores não nulos
            non_null_values = sample_values.dropna().head(10)
            if len(non_null_values) > 0:
                try:
                    # Tenta converter para datetime
                    test_val = str(non_null_values.iloc[0])
                    pd.to_datetime(test_val)
                    # Se funcionou, verifica mais alguns
                    for val in non_null_values.head(5):
                        pd.to_datetime(str(val))
                    return 'DATETIME'
                except:
                    pass
        # Verifica tamanho máximo da string
        if sample_values is not None and len(sample_values) > 0:
            max_len = sample_values.astype(str).str.len().max()
            if pd.isna(max_len) or max_len == 0:
                return 'NVARCHAR(255)'
            elif max_len > 4000:
                return 'NVARCHAR(MAX)'
            else:
                # Adiciona margem de segurança
                return f'NVARCHAR({min(int(max_len * 1.5), 4000)})'
        return 'NVARCHAR(255)'
    elif pandas_type in ['int64', 'int32', 'int16', 'int8']:
        return 'BIGINT'
    elif pandas_type in ['float64', 'float32']:
        return 'FLOAT'
    elif pandas_type == 'bool':
        return 'BIT'
    elif pandas_type in ['datetime64[ns]', 'datetime64']:
        return 'DATETIME'
    else:
        return 'NVARCHAR(255)'

def detect_date_columns(df):
    """Detecta colunas que parecem ser datas"""
    date_columns = []
    for col in df.columns:
        # Verifica se o nome da coluna sugere que é data
        if any(keyword in col.lower() for keyword in ['date', 'timestamp', 'time', '_at', '_on']):
            # Tenta converter uma amostra para verificar
            non_null = df[col].dropna().head(20)
            if len(non_null) > 0:
                try:
                    # Tenta converter alguns valores
                    test_vals = [str(v) for v in non_null.head(10) if str(v).strip()]
                    if test_vals:
                        pd.to_datetime(test_vals[0])
                        date_columns.append(col)
                except:
                    pass
    return date_columns

def create_table_from_csv(cursor, table_name, df):
    """Cria tabela no SQL Server baseada na estrutura do DataFrame"""
    columns = []
    date_cols = detect_date_columns(df)
    
    for col in df.columns:
        col_clean = col.replace(' ', '_').replace('-', '_')
        # Se for uma coluna de data detectada, usa DATETIME
        if col in date_cols:
            sql_type = 'DATETIME'
        else:
            sql_type = infer_sql_type(str(df[col].dtype), df[col])
        columns.append(f"[{col_clean}] {sql_type}")
    
    create_table_sql = f"""
    IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
        DROP TABLE {table_name};
    
    CREATE TABLE {table_name} (
        {', '.join(columns)}
    );
    """
    
    cursor.execute(create_table_sql)
    print(f"  [OK] Tabela '{table_name}' criada com {len(columns)} colunas")
    return date_cols

def insert_data_batch(cursor, connection, table_name, df, date_columns, batch_size=1000):
    """Insere dados em lotes para melhor performance usando executemany"""
    # Limpa nomes de colunas
    df_clean = df.copy()
    df_clean.columns = [col.replace(' ', '_').replace('-', '_') for col in df_clean.columns]
    
    # Converte colunas de data
    date_cols_clean = [col.replace(' ', '_').replace('-', '_') for col in date_columns]
    for col in df_clean.columns:
        if col in date_cols_clean:
            # Tenta converter para datetime, valores inválidos viram NaT
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
    
    # Prepara lista de colunas
    columns = [f"[{col}]" for col in df_clean.columns]
    columns_str = ', '.join(columns)
    placeholders = ', '.join(['?' for _ in columns])
    
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    total_rows = len(df_clean)
    inserted = 0
    
    # Processa em lotes
    for i in range(0, total_rows, batch_size):
        batch = df_clean.iloc[i:i+batch_size]
        
        # Converte DataFrame para lista de tuplas
        rows = []
        for _, row in batch.iterrows():
            values = []
            for idx, val in enumerate(row):
                col_name = df_clean.columns[idx]
                if pd.isna(val) or (isinstance(val, pd.Timestamp) and pd.isna(val)):
                    values.append(None)
                elif isinstance(val, (int, float)) and not pd.isna(val):
                    values.append(val)
                elif isinstance(val, bool):
                    values.append(1 if val else 0)
                elif isinstance(val, (datetime, pd.Timestamp)):
                    # Converte timestamp para datetime do Python
                    if pd.isna(val):
                        values.append(None)
                    else:
                        values.append(val.to_pydatetime() if isinstance(val, pd.Timestamp) else val)
                else:
                    values.append(str(val) if val is not None else None)
            rows.append(tuple(values))
        
        try:
            cursor.executemany(insert_sql, rows)
            inserted += len(batch)
            print(f"  -> Inseridas {inserted}/{total_rows} linhas...", end='\r')
        except Exception as e:
            print(f"\n  [ERRO] Erro ao inserir lote: {str(e)}")
            raise
    
    print(f"\n  [OK] {inserted} linhas inseridas na tabela '{table_name}'")

def import_csv_to_sqlserver(csv_file, connection):
    """Importa um arquivo CSV para o SQL Server"""
    print(f"\n[ARQUIVO] Processando: {csv_file.name}")
    
    try:
        # Lê CSV
        print(f"  -> Lendo arquivo...")
        # Tenta diferentes encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        df = None
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, encoding=encoding, low_memory=False)
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            raise Exception("Nao foi possivel ler o arquivo com nenhum encoding testado")
        
        print(f"  [OK] Arquivo lido: {len(df)} linhas, {len(df.columns)} colunas")
        
        # Cria nome da tabela (remove extensão e limpa)
        table_name = csv_file.stem.replace(' ', '_').replace('-', '_')
        
        # Cria cursor
        cursor = connection.cursor()
        
        # Cria tabela e detecta colunas de data
        print(f"  -> Criando tabela '{table_name}'...")
        date_columns = create_table_from_csv(cursor, table_name, df)
        
        # Insere dados
        print(f"  -> Inserindo dados...")
        insert_data_batch(cursor, connection, table_name, df, date_columns)
        
        # Commit
        connection.commit()
        print(f"  [SUCESSO] '{csv_file.name}' importado com sucesso!\n")
        
    except Exception as e:
        print(f"  [ERRO] Erro ao processar {csv_file.name}: {str(e)}\n")
        connection.rollback()
        raise

def main():
    """Função principal"""
    print("=" * 60)
    print("IMPORTAÇÃO DE CSVs PARA SQL SERVER")
    print("Banco de Dados: Olist Brazilian E-Commerce")
    print("=" * 60)
    
    # Verifica se o diretório existe
    if not os.path.exists(CSV_DIR):
        print(f"[ERRO] Diretorio nao encontrado: {CSV_DIR}")
        return
    
    # Busca CSVs modificados hoje
    print(f"\n[BUSCANDO] Buscando CSVs modificados hoje em: {CSV_DIR}")
    csv_files = get_csvs_modified_today(CSV_DIR)
    
    if not csv_files:
        print("[ERRO] Nenhum arquivo CSV modificado hoje foi encontrado.")
        return
    
    print(f"\n[INFO] Total de arquivos encontrados: {len(csv_files)}\n")
    
    # Conecta ao SQL Server
    print("[CONECTANDO] Conectando ao SQL Server...")
    try:
        conn = pyodbc.connect(get_connection_string())
        print("[OK] Conexao estabelecida!\n")
    except Exception as e:
        print(f"[ERRO] Erro ao conectar ao SQL Server: {str(e)}")
        print("\nDica: Verifique se:")
        print("  - O SQL Server esta rodando")
        print("  - O nome do banco esta correto")
        print("  - O driver ODBC esta instalado")
        print("  - A autenticacao Windows esta configurada")
        return
    
    # Importa cada CSV
    success_count = 0
    error_count = 0
    
    for csv_file in csv_files:
        try:
            import_csv_to_sqlserver(csv_file, conn)
            success_count += 1
        except Exception as e:
            error_count += 1
            print(f"[ERRO] Falha ao importar {csv_file.name}: {str(e)}\n")
    
    # Fecha conexão
    conn.close()
    
    # Resumo
    print("=" * 60)
    print("RESUMO DA IMPORTACAO")
    print("=" * 60)
    print(f"[OK] Sucesso: {success_count} arquivo(s)")
    print(f"[ERRO] Erros: {error_count} arquivo(s)")
    print(f"[INFO] Total processado: {len(csv_files)} arquivo(s)")
    print("=" * 60)

if __name__ == "__main__":
    main()

