"""
Exemplo demonstrando o uso da arquitetura modular
"""

import sys
import os

# Adiciona o diretório pai ao path para importar o módulo src
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collectors.f1_data_collector import F1DataCollector
from src.collectors.session_collector import SessionCollector
from src.collectors.driver_collector import DriverCollector
from src.collectors.position_collector import PositionCollector
from src.api.client import OpenF1Client


def exemplo_uso_modular():
    """Demonstra o uso dos coletores especializados"""
    print("🔧 Exemplo: Usando coletores especializados")
    
    session_key = 9558  # Great Britain 2024 - Race
    
    # Usando cliente compartilhado para otimizar conexões
    with OpenF1Client() as client:
        
        # 1. Coletor de Sessões
        print("\n📅 COLETOR DE SESSÕES:")
        session_collector = SessionCollector(client)
        
        # Busca informações da sessão
        session_info = session_collector.get_session_info(session_key)
        if session_info:
            print(f"  Sessão: {session_info.display_name}")
            print(f"  Circuito: {session_info.circuit_short_name}")
            print(f"  Tipo: {session_info.session_type}")
        
        # Busca corridas de 2023
        races_2023 = session_collector.find_races_by_year(2024)
        print(f"  Corridas em 2023: {len(races_2023)}")
        
        # 2. Coletor de Pilotos
        print("\n👨‍🏎️ COLETOR DE PILOTOS:")
        driver_collector = DriverCollector(client)
        
        # Dados dos pilotos
        drivers_df = driver_collector.collect(session_key)
        print(f"  Pilotos na sessão: {len(drivers_df)}")
        
        # Resumo das equipes
        teams_summary = driver_collector.get_teams_summary(session_key)
        print(f"  Equipes: {len(teams_summary)}")
        for _, team in teams_summary.head(3).iterrows():
            print(f"    {team['team_name']}: {team['driver_acronyms']}")
        
        # Validação dos dados
        driver_validation = driver_collector.validate_driver_data(session_key)
        print(f"  Qualidade dos dados: {driver_validation['total_drivers']} pilotos, "
              f"{driver_validation['missing_photos']} sem foto")
        
        # 3. Coletor de Posições
        print("\n🏁 COLETOR DE POSIÇÕES:")
        position_collector = PositionCollector(client)
        
        # Dados de posições
        positions_df = position_collector.collect(session_key)
        print(f"  Registros de posição: {len(positions_df)}")
        
        # Análise de mudanças
        position_changes = position_collector.get_position_changes(session_key)
        print(f"  Análise de mudanças:")
        for _, change in position_changes.head(3).iterrows():
            change_val = int(change['position_change'])
            change_str = f"+{change_val}" if change_val > 0 else str(change_val)
            print(f"    Piloto #{change['driver_number']}: {change_str} posições")
        
        # Histórico de líderes
        leaders_history = position_collector.get_leaders_history(session_key)
        if not leaders_history.empty:
            leader_changes = len(leaders_history[leaders_history['leader_changed'] == True])
            print(f"  Mudanças de liderança: {leader_changes}")
        
        # Análise de consistência
        consistency = position_collector.get_consistency_analysis(session_key)
        if not consistency.empty:
            most_consistent = consistency.iloc[0]
            print(f"  Piloto mais consistente: #{most_consistent['driver_number']} "
                  f"(índice: {most_consistent['consistency_index']:.2f})")


def exemplo_uso_integrado():
    """Demonstra o uso do coletor principal integrado"""
    print("\n🎯 Exemplo: Usando coletor integrado")
    
    session_key = 9165  # Singapore GP 2023 - Race
    
    with F1DataCollector() as collector:
        # Coleta dados completos
        df = collector.collect_race_data(session_key)
        
        if not df.empty:
            print(f"✅ Dados coletados: {len(df)} registros")
            
            # Análise completa
            analysis = collector.get_race_analysis(session_key)
            
            print("\n📊 ANÁLISE COMPLETA:")
            
            # Timeline summary
            timeline = analysis['timeline_summary']
            if timeline:
                print(f"  Duração: {timeline['session_duration']['duration_minutes']:.1f} min")
                print(f"  Mudanças de liderança: {timeline['leadership_changes']}")
                
                if timeline['biggest_gainer']:
                    print(f"  Maior ganhador: Piloto #{timeline['biggest_gainer']['driver_number']} "
                          f"(+{timeline['biggest_gainer']['positions_gained']} posições)")
                
                if timeline['biggest_loser']:
                    print(f"  Maior perdedor: Piloto #{timeline['biggest_loser']['driver_number']} "
                          f"(-{timeline['biggest_loser']['positions_lost']} posições)")
            
            # Fastest climbers
            climbers = analysis['fastest_climbers']
            if not climbers.empty:
                print(f"\n🚀 MAIORES GANHADORES:")
                for _, climber in climbers.head(3).iterrows():
                    print(f"  Piloto #{climber['driver_number']}: "
                          f"+{climber['position_change']} posições "
                          f"(P{climber['ending_position']} ← P{climber['starting_position']})")
            
            # Consistency analysis
            consistency = analysis['consistency_analysis']
            if not consistency.empty:
                print(f"\n🎯 MAIS CONSISTENTES:")
                for _, driver in consistency.head(3).iterrows():
                    print(f"  Piloto #{driver['driver_number']}: "
                          f"Índice {driver['consistency_index']:.2f} "
                          f"(P{driver['best_position']}-P{driver['worst_position']})")


def exemplo_exportacao_completa():
    """Demonstra exportação completa de análises"""
    print("\n💾 Exemplo: Exportação completa")
    
    session_key = 9161  # Singapore GP 2023 - Qualifying
    
    with F1DataCollector() as collector:
        # Exporta análise completa
        exported_files = collector.export_complete_analysis(
            session_key, 
            "singapore_qualifying_2023"
        )
        
        print("📁 Arquivos exportados:")
        for file_type, filepath in exported_files.items():
            print(f"  {file_type}: {filepath}")
        
        # Validação da qualidade dos dados
        validation = collector.validate_data_quality(session_key)
        print(f"\n🔍 Qualidade geral dos dados: {validation['overall_quality']}")
        
        if validation['drivers_validation']:
            print(f"  Pilotos encontrados: {validation['drivers_validation']['total_drivers']}")
            print(f"  Fotos ausentes: {validation['drivers_validation'].get('missing_photos', 0)}")
        
        if validation['positions_validation']:
            print(f"  Registros de posição: {validation['positions_validation']['total_records']}")
            print(f"  Duração da sessão: {validation['positions_validation']['time_span']['duration_hours']:.2f} horas")


def exemplo_cache_e_performance():
    """Demonstra uso de cache para performance"""
    print("\n⚡ Exemplo: Cache e Performance")
    
    # Com cache habilitado (padrão)
    with F1DataCollector(cache_enabled=True) as collector:
        print("🔄 Primeira requisição (será cacheada):")
        
        import time
        start = time.time()
        df1 = collector.collect_race_data(9165)
        time1 = time.time() - start
        print(f"  Tempo: {time1:.2f}s, Registros: {len(df1)}")
        
        print("⚡ Segunda requisição (do cache):")
        start = time.time()
        df2 = collector.collect_race_data(9165)
        time2 = time.time() - start
        print(f"  Tempo: {time2:.2f}s, Registros: {len(df2)}")
        
        speedup = time1 / time2 if time2 > 0 else float('inf')
        print(f"  Melhoria de performance: {speedup:.1f}x mais rápido")
        
        # Estatísticas do cache
        cache_stats = collector.positions.get_cache_stats()
        print(f"  Entradas no cache: {cache_stats.get('entries_count', 0)}")


def exemplo_busca_avancada():
    """Demonstra recursos de busca avançada"""
    print("\n🔍 Exemplo: Busca Avançada")
    
    with F1DataCollector() as collector:
        # Busca por país
        print("🇧🇷 Sessões do Brasil em 2023:")
        brazil_sessions = collector.sessions.find_sessions_by_country("Brazil", 2023)
        for _, session in brazil_sessions.iterrows():
            print(f"  {session['session_key']} - {session['session_name']} - {session['date_start']}")
        
        # Busca por termo
        print("\n🔍 Busca por 'Monaco':")
        monaco_sessions = collector.sessions.search_sessions("Monaco")
        for _, session in monaco_sessions.head(3).iterrows():
            print(f"  {session['session_key']} - {session['country_name']} {session['year']} - {session['session_name']}")
        
        # Busca de pilotos
        if not brazil_sessions.empty:
            session_key = brazil_sessions.iloc[0]['session_key']
            print(f"\n👤 Busca piloto 'Verstappen' na sessão {session_key}:")
            verstappen = collector.drivers.search_drivers(session_key, "Verstappen")
            if not verstappen.empty:
                driver = verstappen.iloc[0]
                print(f"  {driver['full_name']} - {driver['team_name']} - #{driver['driver_number']}")


def main():
    """Executa todos os exemplos modulares"""
    print("🏎️  F1 DATA COLLECTOR - ARQUITETURA MODULAR")
    print("=" * 60)
    
    try:
        exemplo_uso_modular()
        exemplo_uso_integrado()
        exemplo_exportacao_completa()
        exemplo_cache_e_performance()
        exemplo_busca_avancada()
        
        print("\n✅ Todos os exemplos modulares executados com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro durante execução: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()