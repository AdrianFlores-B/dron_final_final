# Streamlit + HiveMQ Cloud — Versión Final con Filtro de Seguridad de Datos
import streamlit as st
import time, ssl, os, json
from datetime import datetime, date
import pandas as pd
import paho.mqtt.client as mqtt
import base64
import io

# El archivo donde se guardarán los datos de forma permanente
DATA_FILE = "drone_data.csv"

# Nombres de las columnas para consistencia
CSV_COLUMNS = ["ts","lat","lon","alt","drop_id","speed_mps","sats","fix_ok"]

try:
    import pydeck as pdk
    PYDECK_AVAILABLE = True
except ImportError:
    PYDECK_AVAILABLE = False

SUCCESS_SOUND_B64 = "UklGRiQAAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YQAAAAA="

# ==============================================================================
# LÓGICA DE LA APLICACIÓN
# ==============================================================================

def get_pin_source():
    try:
        pin = st.secrets.get("APP_PIN", None)
        if pin: return str(pin).strip()
    except Exception: pass
    pin_env = os.environ.get("APP_PIN", "").strip()
    if pin_env: return pin_env
    return "1234"

APP_PIN = get_pin_source()

def is_editor():
    return st.session_state.get("auth_ok", False)

def login_box():
    with st.sidebar:
        st.markdown("### 🔐 Acceso de edición")
        if not is_editor():
            pin_try = st.text_input("PIN (numérico)", type="password", help="Pide el PIN al administrador")
            if st.button("Ingresar", use_container_width=True):
                if pin_try and pin_try.strip() == APP_PIN:
                    st.session_state.auth_ok = True; st.success("Acceso concedido"); st.rerun()
                else: st.error("PIN incorrecto")
        else:
            st.success("Sesión: Editor")
            if st.button("Cerrar sesión", use_container_width=True):
                st.session_state.auth_ok = False; st.rerun()

# -------- Credenciales y Tópicos MQTT --------
BROKER_HOST    = "3f78afad5f2e407c85dd2eb93951af78.s1.eu.hivemq.cloud"
BROKER_PORT_WS = 8884
BROKER_WS_PATH = "/mqtt"
BROKER_USER    = "AdrianFB"
BROKER_PASS    = "Ab451278"
DEV_ID = "drone-001"
T_CMD      = f"drone/{DEV_ID}/cmd"
T_STATE    = f"drone/{DEV_ID}/state"
T_INFO     = f"drone/{DEV_ID}/info"
T_LOGPART  = f"drone/{DEV_ID}/log/part"
T_EVENTS   = f"drone/{DEV_ID}/events"

st.set_page_config(page_title="Ubicacion y Control de Avispas", layout="wide")
st.title("🛰️ Ubicacion y Control de Avispas")

ss = st.session_state

if "init" not in ss:
    ss.init = True
    ss.mqtt_client = None; ss.mqtt_connected = False
    ss.diag = []; ss.all_data_rows = []; ss.log_chunks = []
    ss.download_in_progress = False; ss.auth_ok = False
    ss.insecure_tls = False; ss.messages = []; ss.info_timestamps = []
    ss.device_online = False; ss.play_sound = False
    
    try:
        if os.path.exists(DATA_FILE):
            df = pd.read_csv(DATA_FILE); ss.all_data_rows = df.to_dict('records')
            ss.diag.append(f"Cargados {len(ss.all_data_rows)} puntos desde {DATA_FILE}")
    except Exception as e: st.error(f"No se pudo cargar {DATA_FILE}: {e}")

# --- Callbacks de MQTT ---
def on_connect(client, userdata, flags, rc, properties=None):
    ss.mqtt_connected = (rc.value == 0)
    if ss.mqtt_connected:
        client.subscribe([(T_STATE,1),(T_INFO,1),(T_LOGPART,1),(T_EVENTS,0)])
        ss.diag.append("Suscrito a tópicos.")

def on_disconnect(client, userdata, rc, properties=None):
    ss.mqtt_connected = False; ss.device_online = False

def on_message(client, userdata, msg):
    try:
        topic = msg.topic; payload = msg.payload.decode("utf-8", errors="ignore")

        if topic == T_INFO:
            now = time.time(); ss.info_timestamps.append(now); ss.info_timestamps = ss.info_timestamps[-2:]
            if len(ss.info_timestamps) == 2 and not ss.device_online and (ss.info_timestamps[1] - ss.info_timestamps[0]) < 11:
                ss.device_online = True; ss.play_sound = True
                ss.messages.append({"type": "success", "text": "✅ ¡Conexión con la ESP32 establecida!"})
        
        elif topic == T_LOGPART:
            try:
                data = json.loads(payload)
                if not data.get('eof', False):
                    if 'data' in data and 'seq' in data: ss.log_chunks.append(data)
                else: # Mensaje de Fin de Archivo (EOF). Aquí ocurre la magia.
                    try:
                        if not ss.log_chunks:
                            ss.messages.append({"type": "warning", "text": "El log del dispositivo está vacío."}); return

                        # 1. Ensamblar el string crudo
                        ss.log_chunks.sort(key=lambda x: x['seq'])
                        raw_csv_string = "".join(chunk['data'] for chunk in ss.log_chunks)

                        ### MODIFICACIÓN CRÍTICA: Filtro de Seguridad ###
                        all_lines = raw_csv_string.strip().splitlines()
                        valid_data_rows = []
                        
                        # 2. Iterar y validar cada línea
                        for line in all_lines:
                            clean_line = line.strip()
                            # Ignorar el encabezado original y líneas vacías
                            if 'ts,lat,lon' in clean_line or not clean_line:
                                continue
                            
                            parts = clean_line.split(',')
                            # Descartar cualquier línea que no tenga 8 columnas
                            if len(parts) == len(CSV_COLUMNS):
                                valid_data_rows.append(parts)
                        
                        total_received = len(all_lines)
                        total_valid = len(valid_data_rows)

                        if not valid_data_rows:
                            ss.messages.append({"type": "warning", "text": f"No se encontraron registros válidos entre las {total_received} líneas recibidas."}); return

                        # 3. Reconstruir un DataFrame perfecto a partir de datos ya validados
                        df_new = pd.DataFrame(valid_data_rows, columns=CSV_COLUMNS)
                        # Convertir columnas a tipos numéricos correctos, por si acaso
                        for col in df_new.columns:
                            df_new[col] = pd.to_numeric(df_new[col], errors='coerce')

                        # 4. Guardar y actualizar la UI
                        df_new.dropna(inplace=True) # Eliminar filas que no se pudieron convertir a número
                        df_new.to_csv(DATA_FILE, index=False)
                        ss.all_data_rows = df_new.to_dict('records')
                        ss.messages.append({"type": "success", "text": f"Log procesado. Se cargaron {len(df_new)} de {total_received} líneas recibidas."})
                        st.toast(f"✅ Log actualizado: {len(df_new)} registros.")
                    
                    except Exception as e:
                        ss.messages.append({"type": "error", "text": f"Error al procesar los datos: {e}"})
                    finally:
                        # 5. Garantizar el desbloqueo de la app
                        ss.download_in_progress = False; ss.log_chunks = []
            
            except json.JSONDecodeError:
                ss.messages.append({"type": "error", "text": "Error: Se recibió un dato corrupto."})
                ss.download_in_progress = False; ss.log_chunks = []

    except Exception as e:
        ss.diag.append(f"Error mayor en on_message: {e}")
        if 'download_in_progress' in ss: ss.download_in_progress = False
        if 'log_chunks' in ss: ss.log_chunks = []

# --- Funciones de Conexión ---
def connect_mqtt():
    if ss.mqtt_client: return
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"st-web-{int(time.time())}", transport="websockets")
        client.on_connect = on_connect; client.on_disconnect = on_disconnect; client.on_message = on_message
        client.username_pw_set(BROKER_USER, BROKER_PASS); client.ws_set_options(path=BROKER_WS_PATH)
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED); client.tls_insecure_set(bool(ss.insecure_tls))
        client.connect(BROKER_HOST, BROKER_PORT_WS, keepalive=60); ss.mqtt_client = client
    except Exception as e: ss.diag.append(f"ERROR AL CONECTAR: {e}"); ss.mqtt_client = None

def disconnect_mqtt():
    if ss.mqtt_client: ss.mqtt_client.disconnect(); ss.mqtt_client = None; ss.mqtt_connected = False; ss.device_online = False

def mqtt_publish(topic, payload_obj):
    if ss.mqtt_client and ss.mqtt_connected:
        try: ss.mqtt_client.publish(topic, json.dumps(payload_obj), qos=1); return True
        except Exception as e: ss.messages.append({"type": "error", "text": f"Error al publicar: {e}"})
    else: ss.messages.append({"type": "warning", "text": "Cliente no conectado."})
    return False

# --- UI ---
login_box()
with st.sidebar:
    st.markdown("---"); st.subheader("Conexión")
    if not ss.mqtt_client:
        if st.button("🔌 Conectar a MQTT", use_container_width=True): connect_mqtt(); st.rerun()
    else:
        if st.button("🔌 Desconectar", use_container_width=True, type="primary"): disconnect_mqtt(); st.rerun()
    st.subheader("Estado del Servidor"); st.success("🟢 Conectado") if ss.mqtt_connected else st.error("🔴 Desconectado")
    st.subheader("Estado del Dispositivo"); st.success("✅ ESP32 Conectada") if ss.get("device_online", False) else st.warning("⚪ Esperando ESP32...")
    st.subheader("Opciones"); ss.insecure_tls = st.checkbox("Usar TLS Inseguro (Debug)", value=ss.insecure_tls)

if ss.mqtt_client: ss.mqtt_client.loop(timeout=0.1)
if ss.get("play_sound", False):
    st.components.v1.html(f'<audio autoplay><source src="data:audio/wav;base64,{SUCCESS_SOUND_B64}" type="audio/wav"></audio>', height=0)
    ss.play_sound = False

st.markdown("---")
left, mid, right = st.columns([1.4,1,1])
message_area = st.empty()

with left:
    st.subheader("Iniciar Mision"); disabled = not is_editor()
    with st.form("start_form", clear_on_submit=False):
        velocity = st.number_input("Velocidad Drone (m/s)", 0.1, 100.0, 10.0, 0.1, disabled=disabled)
        distance = st.number_input("Distancia entre pelotas (m)", 0.1, 1000.0, 30.0, 0.1, disabled=disabled)
        delay_s  = st.number_input("Delay de inicio (s)", 0.0, 120.0, 10.0, 1.0, disabled=disabled)
        step_hz  = st.number_input("Velocidad motor (200 - 1500)", 1, 50000, 200, 10, disabled=disabled)
        try: st.info(f"Intervalo calculado: **{distance/velocity:.2f} s**" if velocity > 0 else "Intervalo: —")
        except: st.info("Intervalo: —")
        if st.form_submit_button("🚀 Actualizar Parámetros", disabled=disabled, use_container_width=True):
            if is_editor():
                payload = {"action":"start", "interval_s": float(distance/velocity), "delay_s": float(delay_s), "step_hz": int(step_hz)}
                if mqtt_publish(T_CMD, payload): ss.messages.append({"type": "success
