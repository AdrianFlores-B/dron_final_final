# Streamlit + HiveMQ Cloud — Versión Estable con Arquitectura de Colas
import streamlit as st
import time, threading, ssl, os, json
from collections import deque
from datetime import datetime, date
import pandas as pd
import paho.mqtt.client as mqtt

try:
    import pydeck as pdk
    PYDECK_AVAILABLE = True
except ImportError:
    PYDECK_AVAILABLE = False

# ==============================================================================
# COLAS SEGURAS PARA COMUNICACIÓN ENTRE HILOS (LA SOLUCIÓN CLAVE)
# ==============================================================================
LOG_QUEUE = deque(maxlen=400)
DATA_QUEUE = deque(maxlen=200)
STATUS_QUEUE = deque(maxlen=5)

# ==============================================================================
# LÓGICA DE LA APLICACIÓN
# ==============================================================================
# -------- HiveMQ Cloud creds (ACTUALIZA ESTOS VALORES) --------
BROKER_HOST    = "pinkhoney-186ef38b.a02.usw2.aws.hivemq.cloud"
BROKER_PORT_WS = 8884
BROKER_WS_PATH = "/mqtt"
BROKER_USER    = "AdrianFB"
BROKER_PASS    = "Ab451278"

DEV_ID = "drone-001"
T_CMD = f"drone/{DEV_ID}/cmd"; T_STATE = f"drone/{DEV_ID}/state"; T_INFO = f"drone/{DEV_ID}/info"; T_PREVIEW = f"drone/{DEV_ID}/preview"; T_LOGPART = f"drone/{DEV_ID}/log/part"; T_EVENTS = f"drone/{DEV_ID}/events"

# -------- HILO DE MQTT (AHORA NO TOCA st.session_state) --------
def mqtt_client_thread(insecure_tls: bool):
    """Función que se ejecuta en un hilo separado para manejar MQTT."""
    def log(message):
        LOG_QUEUE.append(f"{datetime.now().strftime('%H:%M:%S')}  {message}")
    
    def on_connect(c, u, flags, rc, properties=None):
        STATUS_QUEUE.append(rc == 0)
        rc_map = {0: "OK", 1: "Proto incorrecto", 2: "ID cliente inválido", 3: "Servidor no disponible", 4: "Usuario/Pass incorrecto", 5: "No autorizado"}
        log(f"on_connect rc={rc} ({rc_map.get(rc, 'Desconocido')})")
        if rc == 0:
            c.subscribe([(T_STATE,1),(T_INFO,1),(T_PREVIEW,1),(T_LOGPART,1),(T_EVENTS,0)])
            log("Suscrito a tópicos.")

    def on_disconnect(c, u, rc, properties=None):
        STATUS_QUEUE.append(False); log(f"on_disconnect rc={rc}")

    def on_message(c, u, msg):
        try:
            if msg.topic == T_PREVIEW:
                payload = msg.payload.decode("utf-8")
                data = json.loads(payload)
                if isinstance(data, list) and data:
                    DATA_QUEUE.append(data); log(f"Recibidas {len(data)} filas de preview.")
        except Exception as e: log(f"Error procesando mensaje @{msg.topic}: {e}")

    log("Iniciando hilo de cliente MQTT...")
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"st-web-{int(time.time())}", transport="websockets")
        client.username_pw_set(BROKER_USER, BROKER_PASS)
        client.ws_set_options(path=BROKER_WS_PATH)
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.tls_insecure_set(bool(insecure_tls))
        client.on_connect = on_connect; client.on_disconnect = on_disconnect; client.on_message = on_message
        client.reconnect_delay_set(min_delay=1, max_delay=15)
        log(f"Intentando conectar a {BROKER_HOST}...")
        client.connect(BROKER_HOST, BROKER_PORT_WS, keepalive=60)
        client.loop_forever(retry_first_connection=True)
    except Exception as e:
        log(f"EXCEPCIÓN FATAL en hilo MQTT: {type(e).__name__} - {e}"); STATUS_QUEUE.append(False)

# -------- Interfaz de Streamlit --------
st.set_page_config(page_title="Control de Dron", layout="wide")
st.title("🛰️ Ubicacion y Control de Avispas")

ss = st.session_state
if "init" not in ss:
    ss.init = True; ss.mqtt_connected = False; ss.client_thread_started = False; ss.preview_rows = []; ss.diag = []; ss.insecure_tls = False

def process_queues():
    while LOG_QUEUE: ss.diag.append(LOG_QUEUE.popleft())
    ss.diag = ss.diag[-400:]
    while DATA_QUEUE:
        new_rows = DATA_QUEUE.popleft()
        ss.preview_rows.extend(new_rows)
        if len(ss.preview_rows) > 6000: ss.preview_rows = ss.preview_rows[-6000:]
        st.toast(f"🛰️ ¡Se recibieron {len(new_rows)} nuevos puntos de datos!")
    while STATUS_QUEUE: ss.mqtt_connected = STATUS_QUEUE.popleft()

process_queues()

with st.sidebar:
    st.subheader("Conexión")
    if not ss.client_thread_started:
        if st.button("🔌 Conectar a MQTT", use_container_width=True):
            threading.Thread(target=mqtt_client_thread, args=(ss.insecure_tls,), daemon=True).start()
            ss.client_thread_started = True; st.success("Iniciando conexión..."); time.sleep(1); st.rerun()
    else: st.success("Proceso de conexión iniciado.")
    st.subheader("Estado")
    if ss.mqtt_connected: st.success("🟢 Conectado")
    else:
        st.error("🔴 Desconectado")
        if ss.client_thread_started: st.info("Intentando reconectar...")
    st.subheader("Opciones")
    ss.insecure_tls = st.checkbox("Usar TLS Inseguro (Debug)", value=ss.insecure_tls)

st.markdown("---")
st.subheader("Log de Diagnóstico en Tiempo Real")
st.code("\n".join(ss.diag), language="log", height=200)

time.sleep(3)
st.rerun()

