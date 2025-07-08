"""
Script principal para coleta de dados da F1
"""

import logging
import pandas as pd
from src.collectors.f1_data_collector import F1DataCollector

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('f1_data_collector.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def find_available_sessions():
    """Busca e exibe sessões disponíveis"""
    print("🔍 BUSCANDO SESSÕES DISPONÍVEIS")
    print("=" * 50)
    
    with F1DataCollector() as collector:
        # Corridas de 2023
        print("\n🏁 CORRIDAS 2023:")
        races_2023 = collector.find_sessions(2023, session_name="Race")
        if not races_2023.empty:
            for _, session in races_2023.iterrows():
                print(f"  Session Key: {session['session_key']} - {session['country_name']} - {session['session_name']}")
        
        # Qualifying de 2023
        print("\n⏱️ QUALIFYING 2023:")
        quali_2023 = collector.find_sessions(2023, session_name="Qualifying")
        if not quali_2023.empty:
            for _, session in quali_2023.iterrows():
                print(f"  Session Key: {session['session_key']} - {session['country_name']} - {session['session_name']}")
        
        # Brasil 2023
        print("\n🇧🇷 BRASIL 2023 (TODAS AS SESSÕES):")
        brazil_2023 = collector.find_sessions(2023, country_name="Brazil")
        if not brazil_2023.empty:
            for _, session in brazil_2023.iterrows():
                print(f"  Session Key: {session['session_key']} - {session['session_name']} - {session['date_start']}")


def collect_session_data(session_key: int):
    """
    Coleta dados de uma sessão específica
    
    Args:
        session_key: ID da sessão a ser coletada
    """
    print(f"\n📊 COLETANDO DADOS DA SESSÃO {session_key}")
    print("=" * 50)
    
    with F1DataCollector() as collector:
        # Coleta os dados
        df = collector.collect_race_data(session_key)
        
        if df.empty:
            print("❌ Nenhum dado encontrado para esta sessão")
            return None
        
        # Exibe informações básicas
        stats = collector.get_summary_stats(df)
        
        print(f"\n✅ DADOS COLETADOS COM SUCESSO!")
        print(f"   Sessão: {stats['session_info']['name']}")
        print(f"   País: {stats['session_info']['country']}")
        print(f"   Ano: {stats['session_info']['year']}")
        print(f"   Circuito: {stats['session_info']['circuit']}")
        print(f"   Total de registros: {stats['total_records']}")
        print(f"   Pilotos únicos: {stats['unique_drivers']}")
        print(f"   Voltas máximas: {stats['total_laps']}")
        print(f"   Duração: {stats['time_span']['duration_minutes']:.1f} minutos")
        
        # Exibe amostra dos dados
        print(f"\n📋 AMOSTRA DOS DADOS (primeiras 5 linhas):")
        print(df[['driver_acronym', 'team_name', 'position', 'lap_number', 'timestamp']].head())
        
        # Exporta dados
        session_info = collector.get_session_info(session_key)
        if session_info:
            filename = f"{session_info.country_name}_{session_info.year}_{session_info.session_name}".replace(" ", "_")
            
            # Exporta CSV
            csv_path = collector.export_to_csv(df, filename)
            print(f"\n💾 DADOS SALVOS EM CSV: {csv_path}")
            
            # Exporta JSON
            json_path = collector.export_to_json(df, filename)
            print(f"💾 DADOS SALVOS EM JSON: {json_path}")
        
        return df


def analyze_race_positions(df: pd.DataFrame):
    """
    Faz análise básica das posições da corrida
    
    Args:
        df: DataFrame com os dados da corrida
    """
    if df.empty:
        return
    
    print(f"\n📈 ANÁLISE DAS POSIÇÕES")
    print("=" * 30)
    
    # Posições finais (última volta)
    final_lap = df['lap_number'].max()
    final_positions = df[df['lap_number'] == final_lap].sort_values('position')
    
    print(f"\n🏆 POSIÇÕES FINAIS (Volta {final_lap}):")
    for _, row in final_positions.iterrows():
        print(f"  P{row['position']:2d} - {row['driver_acronym']} ({row['team_name']})")
    
    # Mudanças de posição
    print(f"\n🔄 MUDANÇAS DE POSIÇÃO POR PILOTO:")
    position_changes = df.groupby('driver_acronym').agg({
        'position': ['min', 'max', 'first', 'last'],
        'team_name': 'first'
    }).round(0)
    
    for driver in position_changes.index:
        pos_min = int(position_changes.loc[driver, ('position', 'min')])
        pos_max = int(position_changes.loc[driver, ('position', 'max')])
        pos_start = int(position_changes.loc[driver, ('position', 'first')])
        pos_end = int(position_changes.loc[driver, ('position', 'last')])
        team = position_changes.loc[driver, ('team_name', 'first')]
        
        change = pos_start - pos_end  # Positivo = ganhou posições
        change_str = f"+{change}" if change > 0 else str(change)
        
        print(f"  {driver:3s} ({team:12s}) - Início: P{pos_start:2d} | Fim: P{pos_end:2d} | Melhor: P{pos_min:2d} | Pior: P{pos_max:2d} | Mudança: {change_str}")


def main():
    """Função principal"""
    print("🏎️  F1 DATA COLLECTOR")
    print("=" * 50)
    
    # Escolha do modo de operação
    mode = input("""
Escolha uma opção:
1 - Buscar sessões disponíveis
2 - Coletar dados de uma sessão específica
3 - Exemplo completo (Singapore GP 2023)

Digite sua escolha (1, 2 ou 3): """).strip()
    
    if mode == "1":
        find_available_sessions()
    
    elif mode == "2":
        try:
            session_key = int(input("\nDigite o session_key: "))
            df = collect_session_data(session_key)
            
            if df is not None and not df.empty:
                analyze = input("\nDeseja fazer análise das posições? (s/n): ").strip().lower()
                if analyze == 's':
                    analyze_race_positions(df)
        
        except ValueError:
            print("❌ Session key deve ser um número inteiro")
    
    elif mode == "3":
        print("\n🇸🇬 EXEMPLO: Singapore GP 2023 - Race")
        session_key = 9165  # Singapore GP 2023 Race
        
        df = collect_session_data(session_key)
        if df is not None and not df.empty:
            analyze_race_positions(df)
    
    else:
        print("❌ Opção inválida")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Operação cancelada pelo usuário")
    except Exception as e:
        logger.error(f"Erro não esperado: {e}")
        print(f"❌ Erro: {e}")