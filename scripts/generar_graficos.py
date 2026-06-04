#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

RESULTS_DIR = Path("resulta2")
OUTPUT_DIR = Path("graficos")
OUTPUT_DIR.mkdir(exist_ok=True)

plt.style.use('seaborn-v0_8-whitegrid')
COLORS = {
    'primary': '#2E86AB',
    'secondary': '#A23B72',
    'success': '#28A745',
    'danger': '#DC3545',
    'warning': '#FFC107',
    'info': '#17A2B8',
    'neutral': '#6C757D'
}


def load_json(filename):
    filepath = RESULTS_DIR / filename
    if not filepath.exists():
        print(f"⚠️  Advertencia: {filepath} no existe")
        return None
    with open(filepath, 'r') as f:
        return json.load(f)


def setup_plot(title, xlabel, ylabel, figsize=(10, 6)):
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_title(title, fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    return fig, ax


def save_plot(fig, filename):
    fig.savefig(OUTPUT_DIR / filename, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"✓ Generado: {OUTPUT_DIR / filename}")


def add_value_labels(ax, spacing=5):
    for rect in ax.patches:
        y_value = rect.get_height()
        x_value = rect.get_x() + rect.get_width() / 2
        label = f"{y_value:.1f}" if isinstance(y_value, float) else str(int(y_value))
        ax.annotate(
            label,
            (x_value, y_value),
            xytext=(0, spacing),
            textcoords="offset points",
            ha='center',
            va='bottom'
        )


def generar_caida_original():
    t1_data = load_json("Tarea1_sincrono.json")
    kafka_data = load_json("kafka-1-consumer.json")
    
    if not t1_data or not kafka_data:
        return
    
    sincronicas_fallidas = t1_data['resultados']['success_rate']['fallidas']
    kafka_fallidas = kafka_data['resultados']['throughput']['total_consultas'] - kafka_data['resultados']['throughput']['exitosas']
    
    fig, ax = setup_plot(
        'Consultas Fallidas durante Caída del Sistema',
        'Arquitectura',
        'Consultas Fallidas'
    )
    
    bars = ax.bar(['Síncrona', 'Kafka'], 
                  [sincronicas_fallidas, kafka_fallidas],
                  color=[COLORS['danger'], COLORS['success']])
    
    add_value_labels(ax)
    save_plot(fig, 'caida_original.png')


def generar_stats_1consumidor():
    normal = load_json("kafka-1-consumer.json")
    failure = load_json("kafka-failure-1-consumer.json")
    
    if not normal or not failure:
        return
    
    fig, ax = setup_plot(
        'Backlog durante Caída - 1 Consumer',
        'Escenario',
        'Mensajes en Cola'
    )
    
    x = np.arange(2)
    width = 0.35
    
    queries = [normal['resultados']['backlog_size']['peak_lag_queries'],
               failure['resultados']['backlog_size']['peak_lag_queries']]
    retry = [normal['resultados']['backlog_size']['peak_lag_retry'],
             failure['resultados']['backlog_size']['peak_lag_retry']]
    
    bars1 = ax.bar(x - width/2, queries, width, label='Queries', color=COLORS['primary'])
    bars2 = ax.bar(x + width/2, retry, width, label='Retry', color=COLORS['warning'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(['Normal', 'Con Falla'])
    ax.legend()
    add_value_labels(ax)
    save_plot(fig, 'stats_1consumidor.png')


def generar_stats_5consumidores():
    normal = load_json("kafka-5-consumers.json")
    failure = load_json("kafka-failure-5-consumers.json")
    
    if not normal or not failure:
        return
    
    fig, ax = setup_plot(
        'Backlog durante Caída - 5 Consumers',
        'Escenario',
        'Mensajes en Cola'
    )
    
    x = np.arange(2)
    width = 0.35
    
    queries = [normal['resultados']['backlog_size']['peak_lag_queries'],
               failure['resultados']['backlog_size']['peak_lag_queries']]
    retry = [normal['resultados']['backlog_size']['peak_lag_retry'],
             failure['resultados']['backlog_size']['peak_lag_retry']]
    
    bars1 = ax.bar(x - width/2, queries, width, label='Queries', color=COLORS['primary'])
    bars2 = ax.bar(x + width/2, retry, width, label='Retry', color=COLORS['warning'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(['Normal', 'Con Falla'])
    ax.legend()
    add_value_labels(ax)
    save_plot(fig, 'stats_5consumidores.png')


def generar_stats_10consumidores():
    normal = load_json("kafka-10-consumers.json")
    failure = load_json("kafka-failure-10-consumers.json")
    
    if not normal or not failure:
        return
    
    fig, ax = setup_plot(
        'Backlog durante Caída - 10 Consumers',
        'Escenario',
        'Mensajes en Cola'
    )
    
    x = np.arange(2)
    width = 0.35
    
    queries = [normal['resultados']['backlog_size']['peak_lag_queries'],
               failure['resultados']['backlog_size']['peak_lag_queries']]
    retry = [normal['resultados']['backlog_size']['peak_lag_retry'],
             failure['resultados']['backlog_size']['peak_lag_retry']]
    
    bars1 = ax.bar(x - width/2, queries, width, label='Queries', color=COLORS['primary'])
    bars2 = ax.bar(x + width/2, retry, width, label='Retry', color=COLORS['warning'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(['Normal', 'Con Falla'])
    ax.legend()
    add_value_labels(ax)
    save_plot(fig, 'stats_10consumidores.png')


def generar_throughput():
    data_1 = load_json("kafka-1-consumer.json")
    data_5 = load_json("kafka-5-consumers.json")
    data_10 = load_json("kafka-10-consumers.json")
    
    if not all([data_1, data_5, data_10]):
        return
    
    fig, ax = setup_plot(
        'Throughput por Cantidad de Consumers',
        'Cantidad de Consumers',
        'Throughput (consultas/segundo)'
    )
    
    consumers = ['1', '5', '10']
    throughput = [
        data_1['resultados']['throughput']['throughput_qps'],
        data_5['resultados']['throughput']['throughput_qps'],
        data_10['resultados']['throughput']['throughput_qps']
    ]
    
    bars = ax.bar(consumers, throughput, color=COLORS['primary'])
    add_value_labels(ax)
    save_plot(fig, 'throughput.png')


def generar_grafico_reintentos():
    data_1 = load_json("kafka-1-consumer.json")
    data_5 = load_json("kafka-5-consumers.json")
    data_10 = load_json("kafka-10-consumers.json")
    
    if not all([data_1, data_5, data_10]):
        return
    
    fig, ax = setup_plot(
        'Retry Rate por Cantidad de Consumers',
        'Cantidad de Consumers',
        'Retry Rate (%)'
    )
    
    consumers = ['1', '5', '10']
    retry_rate = [
        data_1['resultados']['retry_rate']['retry_rate_porcentaje'],
        data_5['resultados']['retry_rate']['retry_rate_porcentaje'],
        data_10['resultados']['retry_rate']['retry_rate_porcentaje']
    ]
    
    bars = ax.bar(consumers, retry_rate, color=COLORS['warning'])
    add_value_labels(ax)
    save_plot(fig, 'grafico_reintentos.png')


def generar_grafico_fallas():
    data_1 = load_json("kafka-1-consumer.json")
    data_5 = load_json("kafka-5-consumers.json")
    data_10 = load_json("kafka-10-consumers.json")
    
    if not all([data_1, data_5, data_10]):
        return
    
    fig, ax = setup_plot(
        'DLQ Rate por Cantidad de Consumers',
        'Cantidad de Consumers',
        'DLQ Rate (%)'
    )
    
    consumers = ['1', '5', '10']
    dlq_rate = [
        data_1['resultados']['dlq_rate']['dlq_rate_porcentaje'],
        data_5['resultados']['dlq_rate']['dlq_rate_porcentaje'],
        data_10['resultados']['dlq_rate']['dlq_rate_porcentaje']
    ]
    
    bars = ax.bar(consumers, dlq_rate, color=COLORS['danger'])
    add_value_labels(ax)
    save_plot(fig, 'grafico_fallas.png')


def generar_backlog_size():
    normal_1 = load_json("kafka-1-consumer.json")
    normal_5 = load_json("kafka-5-consumers.json")
    normal_10 = load_json("kafka-10-consumers.json")
    failure_1 = load_json("kafka-failure-1-consumer.json")
    failure_5 = load_json("kafka-failure-5-consumers.json")
    failure_10 = load_json("kafka-failure-10-consumers.json")
    
    if not all([normal_1, normal_5, normal_10, failure_1, failure_5, failure_10]):
        return
    
    fig, ax = setup_plot(
        'Backlog: Normal vs Con Falla',
        'Cantidad de Consumers',
        'Peak Lag Queries'
    )
    
    x = np.arange(3)
    width = 0.35
    
    normal = [
        normal_1['resultados']['backlog_size']['peak_lag_queries'],
        normal_5['resultados']['backlog_size']['peak_lag_queries'],
        normal_10['resultados']['backlog_size']['peak_lag_queries']
    ]
    failure = [
        failure_1['resultados']['backlog_size']['peak_lag_queries'],
        failure_5['resultados']['backlog_size']['peak_lag_queries'],
        failure_10['resultados']['backlog_size']['peak_lag_queries']
    ]
    
    bars1 = ax.bar(x - width/2, normal, width, label='Normal', color=COLORS['success'])
    bars2 = ax.bar(x + width/2, failure, width, label='Con Falla', color=COLORS['danger'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(['1', '5', '10'])
    ax.legend()
    add_value_labels(ax)
    save_plot(fig, 'backlog_size.png')


def generar_grafico_recovery():
    failure_1 = load_json("kafka-failure-1-consumer.json")
    failure_5 = load_json("kafka-failure-5-consumers.json")
    failure_10 = load_json("kafka-failure-10-consumers.json")
    
    if not all([failure_1, failure_5, failure_10]):
        return
    
    fig, ax = setup_plot(
        'Tiempo de Recuperación tras Falla',
        'Cantidad de Consumers',
        'Tiempo de Recuperación (segundos)'
    )
    
    consumers = ['1', '5', '10']
    recovery_time = [
        failure_1['resultados']['recovery_time']['recovery_time_seconds'],
        failure_5['resultados']['recovery_time']['recovery_time_seconds'],
        failure_10['resultados']['recovery_time']['recovery_time_seconds']
    ]
    
    bars = ax.bar(consumers, recovery_time, color=COLORS['success'])
    add_value_labels(ax)
    save_plot(fig, 'grafico_recovery.png')


def generar_resultados_t1():
    t1_data = load_json("Tarea1_sincrono.json")
    kafka_data = load_json("kafka-5-consumers.json")
    
    if not t1_data or not kafka_data:
        return
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    fig.suptitle('Comparación: Tarea 1 (Síncrono) vs Tarea 2 (Kafka)', 
                 fontsize=16, fontweight='bold')
    
    metrics_1 = ['Throughput\n(qps)', 'Success Rate\n(%)']
    t1_values = [
        t1_data['resultados']['throughput']['throughput_qps'],
        t1_data['resultados']['success_rate']['success_rate_porcentaje']
    ]
    kafka_values = [
        kafka_data['resultados']['throughput']['throughput_qps'],
        (kafka_data['resultados']['throughput']['exitosas'] / 
         kafka_data['resultados']['throughput']['total_consultas']) * 100
    ]
    
    x = np.arange(len(metrics_1))
    width = 0.35
    
    bars1 = ax1.bar(x - width/2, t1_values, width, label='Tarea 1', color=COLORS['success'])
    bars2 = ax1.bar(x + width/2, kafka_values, width, label='Kafka', color=COLORS['primary'])
    
    ax1.set_ylabel('Valor')
    ax1.set_xticks(x)
    ax1.set_xticklabels(metrics_1)
    ax1.legend()
    
    metrics_2 = ['Latencia p50\n(ms)', 'Latencia p95\n(ms)']
    t1_lat = [
        t1_data['resultados']['latencia']['latencia_p50_ms'],
        t1_data['resultados']['latencia']['latencia_p95_ms']
    ]
    kafka_lat = [
        kafka_data['resultados']['latencia']['latencia_p50_ms'],
        kafka_data['resultados']['latencia']['latencia_p95_ms']
    ]
    
    bars3 = ax2.bar(x - width/2, t1_lat, width, label='Tarea 1', color=COLORS['success'])
    bars4 = ax2.bar(x + width/2, kafka_lat, width, label='Kafka', color=COLORS['primary'])
    
    ax2.set_ylabel('Latencia (ms) - Escala Logarítmica')
    ax2.set_yscale('log')
    ax2.set_xticks(x)
    ax2.set_xticklabels(metrics_2)
    ax2.legend()
    
    plt.tight_layout()
    save_plot(fig, 'resultados_t1.png')


def generar_no_reintentos_throughput():
    data_1 = load_json("kafka-1-consumer.json")
    data_5 = load_json("kafka-5-consumers.json")
    data_10 = load_json("kafka-10-consumers.json")
    no_retry_1 = load_json("kafka-no-retry-1-consumer.json")
    no_retry_5 = load_json("kafka-no-retry-5-consumers.json")
    no_retry_10 = load_json("kafka-no-retry-10-consumers.json")
    
    if not all([data_1, data_5, data_10, no_retry_1, no_retry_5, no_retry_10]):
        return
    
    fig, ax = setup_plot(
        'Throughput: Con vs Sin Reintentos',
        'Cantidad de Consumers',
        'Throughput (consultas/segundo)'
    )
    
    x = np.arange(3)
    width = 0.35
    consumers = ['1', '5', '10']
    
    throughput_with = [
        data_1['resultados']['throughput']['throughput_qps'],
        data_5['resultados']['throughput']['throughput_qps'],
        data_10['resultados']['throughput']['throughput_qps']
    ]
    throughput_without = [
        no_retry_1['resultados']['throughput']['throughput_qps'],
        no_retry_5['resultados']['throughput']['throughput_qps'],
        no_retry_10['resultados']['throughput']['throughput_qps']
    ]
    
    bars1 = ax.bar(x - width/2, throughput_with, width, label='Con Reintentos', color=COLORS['primary'])
    bars2 = ax.bar(x + width/2, throughput_without, width, label='Sin Reintentos', color=COLORS['neutral'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(consumers)
    ax.legend()
    add_value_labels(ax)
    save_plot(fig, 'no_reintentos_throughput.png')


def generar_no_reintentos_latencia():
    data_1 = load_json("kafka-1-consumer.json")
    data_5 = load_json("kafka-5-consumers.json")
    data_10 = load_json("kafka-10-consumers.json")
    no_retry_1 = load_json("kafka-no-retry-1-consumer.json")
    no_retry_5 = load_json("kafka-no-retry-5-consumers.json")
    no_retry_10 = load_json("kafka-no-retry-10-consumers.json")
    
    if not all([data_1, data_5, data_10, no_retry_1, no_retry_5, no_retry_10]):
        return
    
    fig, ax = setup_plot(
        'Latencia p50: Con vs Sin Reintentos',
        'Cantidad de Consumers',
        'Latencia p50 (segundos)'
    )
    
    x = np.arange(3)
    width = 0.35
    consumers = ['1', '5', '10']
    
    latency_with = [
        data_1['resultados']['latencia']['latencia_p50_ms'] / 1000,
        data_5['resultados']['latencia']['latencia_p50_ms'] / 1000,
        data_10['resultados']['latencia']['latencia_p50_ms'] / 1000
    ]
    latency_without = [
        no_retry_1['resultados']['latencia']['latencia_p50_ms'] / 1000,
        no_retry_5['resultados']['latencia']['latencia_p50_ms'] / 1000,
        no_retry_10['resultados']['latencia']['latencia_p50_ms'] / 1000
    ]
    
    bars1 = ax.bar(x - width/2, latency_with, width, label='Con Reintentos', color=COLORS['primary'])
    bars2 = ax.bar(x + width/2, latency_without, width, label='Sin Reintentos', color=COLORS['neutral'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(consumers)
    ax.legend()
    add_value_labels(ax)
    save_plot(fig, 'no_reintentos_latencia.png')


def generar_no_reintentos_exitosas():
    data_1 = load_json("kafka-1-consumer.json")
    data_5 = load_json("kafka-5-consumers.json")
    data_10 = load_json("kafka-10-consumers.json")
    no_retry_1 = load_json("kafka-no-retry-1-consumer.json")
    no_retry_5 = load_json("kafka-no-retry-5-consumers.json")
    no_retry_10 = load_json("kafka-no-retry-10-consumers.json")
    
    if not all([data_1, data_5, data_10, no_retry_1, no_retry_5, no_retry_10]):
        return
    
    fig, ax = setup_plot(
        'Consultas Exitosas: Con vs Sin Reintentos',
        'Cantidad de Consumers',
        'Consultas Exitosas Totales'
    )
    
    x = np.arange(3)
    width = 0.35
    consumers = ['1', '5', '10']
    
    exitosas_with = [
        data_1['resultados']['throughput']['exitosas'],
        data_5['resultados']['throughput']['exitosas'],
        data_10['resultados']['throughput']['exitosas']
    ]
    exitosas_without = [
        no_retry_1['resultados']['throughput']['exitosas'],
        no_retry_5['resultados']['throughput']['exitosas'],
        no_retry_10['resultados']['throughput']['exitosas']
    ]
    
    bars1 = ax.bar(x - width/2, exitosas_with, width, label='Con Reintentos', color=COLORS['primary'])
    bars2 = ax.bar(x + width/2, exitosas_without, width, label='Sin Reintentos', color=COLORS['neutral'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(consumers)
    ax.legend()
    add_value_labels(ax)
    save_plot(fig, 'no_reintentos_exitosas.png')


def generar_spike():
    normal_1 = load_json("kafka-1-consumer.json")
    normal_5 = load_json("kafka-5-consumers.json")
    normal_10 = load_json("kafka-10-consumers.json")
    spike_1 = load_json("kafka-spike-1-consumer.json")
    spike_5 = load_json("kafka-spike-5-consumers.json")
    spike_10 = load_json("kafka-spike-10-consumers.json")
    
    if not all([normal_1, normal_5, normal_10, spike_1, spike_5, spike_10]):
        return
    
    fig, ax = setup_plot(
        'Latencia: Normal vs Spike de Tráfico',
        'Cantidad de Consumers',
        'Latencia p50 (segundos)'
    )
    
    x = np.arange(3)
    width = 0.35
    
    normal = [
        normal_1['resultados']['latencia']['latencia_p50_ms'] / 1000,
        normal_5['resultados']['latencia']['latencia_p50_ms'] / 1000,
        normal_10['resultados']['latencia']['latencia_p50_ms'] / 1000
    ]
    spike = [
        spike_1['resultados']['latencia']['latencia_p50_ms'] / 1000,
        spike_5['resultados']['latencia']['latencia_p50_ms'] / 1000,
        spike_10['resultados']['latencia']['latencia_p50_ms'] / 1000
    ]
    
    bars1 = ax.bar(x - width/2, normal, width, label='Normal', color=COLORS['primary'])
    bars2 = ax.bar(x + width/2, spike, width, label='Spike', color=COLORS['danger'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(['1', '5', '10'])
    ax.legend()
    add_value_labels(ax)
    save_plot(fig, 'spike.png')


def generar_latencia_normal():
    data_1 = load_json("kafka-1-consumer.json")
    data_5 = load_json("kafka-5-consumers.json")
    data_10 = load_json("kafka-10-consumers.json")
    
    if not all([data_1, data_5, data_10]):
        return
    
    fig, ax = setup_plot(
        'Latencia en Escenario Normal',
        'Cantidad de Consumers',
        'Latencia (milisegundos)'
    )
    
    x = np.arange(3)
    width = 0.35
    
    p50 = [
        data_1['resultados']['latencia']['latencia_p50_ms'],
        data_5['resultados']['latencia']['latencia_p50_ms'],
        data_10['resultados']['latencia']['latencia_p50_ms']
    ]
    p95 = [
        data_1['resultados']['latencia']['latencia_p95_ms'],
        data_5['resultados']['latencia']['latencia_p95_ms'],
        data_10['resultados']['latencia']['latencia_p95_ms']
    ]
    
    bars1 = ax.bar(x - width/2, p50, width, label='p50', color=COLORS['primary'])
    bars2 = ax.bar(x + width/2, p95, width, label='p95', color=COLORS['warning'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(['1', '5', '10'])
    ax.legend()
    add_value_labels(ax)
    save_plot(fig, 'latencia_normal.png')


def generar_backlog_spike():
    spike_1 = load_json("kafka-spike-1-consumer.json")
    spike_5 = load_json("kafka-spike-5-consumers.json")
    spike_10 = load_json("kafka-spike-10-consumers.json")
    
    if not all([spike_1, spike_5, spike_10]):
        return
    
    fig, ax = setup_plot(
        'Backlog durante Spike de Tráfico',
        'Cantidad de Consumers',
        'Mensajes en Cola'
    )
    
    x = np.arange(3)
    width = 0.35
    
    queries = [
        spike_1['resultados']['backlog_size']['peak_lag_queries'],
        spike_5['resultados']['backlog_size']['peak_lag_queries'],
        spike_10['resultados']['backlog_size']['peak_lag_queries']
    ]
    retry = [
        spike_1['resultados']['backlog_size']['peak_lag_retry'],
        spike_5['resultados']['backlog_size']['peak_lag_retry'],
        spike_10['resultados']['backlog_size']['peak_lag_retry']
    ]
    
    bars1 = ax.bar(x - width/2, queries, width, label='Queries', color=COLORS['primary'])
    bars2 = ax.bar(x + width/2, retry, width, label='Retry', color=COLORS['warning'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(['1', '5', '10'])
    ax.legend()
    add_value_labels(ax)
    save_plot(fig, 'backlog_spike.png')


def generar_backlog_normal():
    data_1 = load_json("kafka-1-consumer.json")
    data_5 = load_json("kafka-5-consumers.json")
    data_10 = load_json("kafka-10-consumers.json")
    
    if not all([data_1, data_5, data_10]):
        return
    
    fig, ax = setup_plot(
        'Backlog en Escenario Normal',
        'Cantidad de Consumers',
        'Mensajes en Cola'
    )
    
    x = np.arange(3)
    width = 0.35
    
    queries = [
        data_1['resultados']['backlog_size']['peak_lag_queries'],
        data_5['resultados']['backlog_size']['peak_lag_queries'],
        data_10['resultados']['backlog_size']['peak_lag_queries']
    ]
    retry = [
        data_1['resultados']['backlog_size']['peak_lag_retry'],
        data_5['resultados']['backlog_size']['peak_lag_retry'],
        data_10['resultados']['backlog_size']['peak_lag_retry']
    ]
    
    bars1 = ax.bar(x - width/2, queries, width, label='Queries', color=COLORS['primary'])
    bars2 = ax.bar(x + width/2, retry, width, label='Retry', color=COLORS['warning'])
    
    ax.set_xticks(x)
    ax.set_xticklabels(['1', '5', '10'])
    ax.legend()
    add_value_labels(ax)
    save_plot(fig, 'backlog_normal.png')


if __name__ == "__main__":
    print("Generando gráficos...")
    print("=" * 50)
    
    generar_caida_original()
    generar_stats_1consumidor()
    generar_stats_5consumidores()
    generar_stats_10consumidores()
    generar_throughput()
    generar_grafico_reintentos()
    generar_grafico_fallas()
    generar_backlog_size()
    generar_grafico_recovery()
    generar_resultados_t1()
    generar_no_reintentos_throughput()
    generar_no_reintentos_latencia()
    generar_no_reintentos_exitosas()
    generar_spike()
    generar_latencia_normal()
    generar_backlog_spike()
    generar_backlog_normal()
    
    print("=" * 50)
    print(f"✓ Todos los gráficos generados en {OUTPUT_DIR}/")
