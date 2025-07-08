"""
Exemplo básico de uso do F1 Data Collector
"""

import sys
import os

# Adiciona o diretório pai ao path para importar o módulo src
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collectors.f1_data_collector import F1DataCollector


def exemplo_buscar_sessoes():
    """Exemplo: Buscar sessões disponíveis"""
    print("🔍 Exemplo: Buscando sessões de 2023")
    
    with F1DataCollector() as collector:
        # Busca todas as corridas de 2024
        df_races = collector.find_sessions(year=2025, session_name="Race")
        
        print(f"Encontradas {len(df_races)} corridas em 2025:")
        for _, race in df_races.iterrows():
            print(f"  {race['session_key']} - {race['country_name']} - {race['date_start']}")


def exemplo_coletar_dados_sessao():
    """Exemplo: Coletar dados de uma sessão específica"""
    print("\n📊 Exemplo: Coletando dados do Great Britain 2024 - Race")
    
    session_key = 9947  # Great Britain 2025 - Race
    
    with F1DataCollector() as collector:
        # Coleta os dados
        df = collector.collect_race_data(session_key)
        
        if df.empty:
            print("Nenhum dado encontrado")
            return
        
        print(f"✅ Dados coletados: {len(df)} registros")
        
        # Exibe informações básicas
        print("\nInformações da sessão:")
        print(f"  País: {df['country_name'].iloc[0]}")
        print(f"  Circuito: {df['circuit_name'].iloc[0]}")
        print(f"  Tipo: {df['session_name'].iloc[0]}")
        print(f"  Pilotos: {df['driver_number'].nunique()}")
        print(f"  Voltas: {df['lap_number'].max()}")
        
        # Amostra dos dados
        print("\nAmostra dos dados:")
        print(df[['driver_acronym', 'team_name', 'position', 'lap_number']].head(10))
        
        return df


def exemplo_exportar_dados():
    """Exemplo: Exportar dados para arquivos"""
    print("\n💾 Exemplo: Exportando dados")
    
    session_key = 9947  # Great Britain 2025 - Race
    
    with F1DataCollector() as collector:
        df = collector.collect_race_data(session_key)
        
        if df.empty:
            print("Nenhum dado para exportar")
            return
        
        # Exporta para CSV
        csv_file = collector.export_to_csv(df, "singapore_qualifying_2023")
        print(f"CSV salvo em: {csv_file}")
        
        # Exporta para JSON
        json_file = collector.export_to_json(df, "singapore_qualifying_2023")
        print(f"JSON salvo em: {json_file}")


def exemplo_analise_basica(df):
    """Exemplo: Análise básica dos dados"""
    if df is None or df.empty:
        return
        
    print("\n📈 Exemplo: Análise básica dos dados")
    
    # Estatísticas por piloto
    driver_stats = df.groupby(['driver_acronym', 'team_name']).agg({
        'position': ['min', 'max', 'mean'],
        'lap_number': 'count'
    }).round(2)
    
    print("\nEstatísticas por piloto:")
    print("Driver | Team         | Melhor | Pior | Média | Registros")
    print("-" * 60)
    
    for driver_acronym in driver_stats.index.get_level_values(0):
        team = driver_stats.loc[driver_acronym].index[0]
        stats = driver_stats.loc[(driver_acronym, team)]
        
        pos_min = int(stats[('position', 'min')])
        pos_max = int(stats[('position', 'max')])
        pos_mean = stats[('position', 'mean')]
        count = int(stats[('lap_number', 'count')])
        
        print(f"{driver_acronym:6s} | {team[:12]:12s} | P{pos_min:2d}    | P{pos_max:2d}  | {pos_mean:5.1f} | {count:9d}")


def exemplo_filtros_dados():
    """Exemplo: Aplicando filtros nos dados"""
    print("\n🔍 Exemplo: Filtros e consultas nos dados")
    
    session_key = 9558  # Great Britain 2024 - Race
    
    with F1DataCollector() as collector:
        df = collector.collect_race_data(session_key)
        
        if df.empty:
            return
        
        # Filtro 1: Apenas Verstappen
        verstappen_data = df[df['driver_acronym'] == 'VER']
        print(f"\nRegistros do Verstappen: {len(verstappen_data)}")
        print(f"Posições: {verstappen_data['position'].min()} a {verstappen_data['position'].max()}")
        
        # Filtro 2: Top 3 na última volta
        last_lap = df['lap_number'].max()
        top3_last_lap = df[
            (df['lap_number'] == last_lap) & 
            (df['position'] <= 3)
        ].sort_values('position')
        
        print(f"\nTop 3 na volta {last_lap}:")
        for _, row in top3_last_lap.iterrows():
            print(f"  P{row['position']} - {row['driver_acronym']} ({row['team_name']})")
        
        # Filtro 3: Pilotos que estiveram em P1
        leaders = df[df['position'] == 1]['driver_acronym'].unique()
        print(f"\nPilotos que lideraram: {', '.join(leaders)}")


def main():
    """Executa todos os exemplos"""
    print("🏎️  F1 DATA COLLECTOR - EXEMPLOS DE USO")
    print("=" * 50)
    
    try:
        # 1. Buscar sessões
        exemplo_buscar_sessoes()
        
        # 2. Coletar dados
        df = exemplo_coletar_dados_sessao()
        
        # 3. Análise básica
        exemplo_analise_basica(df)
        
        # 4. Filtros
        exemplo_filtros_dados()
        
        # 5. Exportar
        exemplo_exportar_dados()
        
        print("\n✅ Todos os exemplos executados com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro durante execução: {e}")


if __name__ == "__main__":
    main()