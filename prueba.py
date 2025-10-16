# =========================================================
# Streamlit + HiveMQ Cloud — Versión robusta con Log Completo
# Compatible con ESP32-S3 (stream_log por chunks arbitrarios)
# =========================================================

import streamlit as st
import time, ssl, os, json, io, base64
from datetime import datetime, date
import pandas as pd
import paho.mqtt.client as mqtt

# --- Archivo donde se guardan los datos ---
DATA_FILE = "drone_data.csv"

# --- Pydeck opcional ---
try:
    import pydeck as pdk
    PYDECK_AVAILABLE = True
except ImportError:
    PYDECK_AVAILABLE = False

# --- Sonido base64 (tiny WAV) ---
SUCCESS_SOUND_B64 = "UklGRiQAAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YQAAAAA="

# =========================================================
# LOGIN SIMPLE POR PIN
# =========================================================
def get_pin_source():
    try:
        pin = st.secrets.get("APP_PIN", None)
        if pin:
            return str(pin).strip()
    except Exception:
        pass
    pin_env = os.environ.get("APP_PIN", "").strip()
    if pin_env:
        return pin_env
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
                    st.session_state.auth_ok = True
                    st.success("Acceso concedido")
                    st.rerun()
                else:
                    st.error("PIN incorrecto")
        else:
            st.success("Sesión: Editor")
            if st.button("Cerrar sesión", use_container_width=True):
                st.session_state.auth_ok = False
                st.rerun()

# =========================================================
# MQTT CONFIG (HiveMQ Cloud)
# =========================================================
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

# =========================================================
# INICIALIZACIÓN DE SESIÓN
# =========================================================
st.set_page_config(page_title="Ubicacion y Control de Avispas", layout="wide")
st.title("🛰️ Ubicacion y Control de Avispas")

ss = st.session_state
if "init" not in ss:
    ss.init = True
    ss.mqtt_client = None
    ss.mqtt_connected = False
    ss.diag = []
    ss.all_data_rows = []
    ss.log_chunks = []
    ss.log_seq_last = -1
    ss.log_eof = False
    ss.download_in_progress = False
    ss.auth_ok = False
    ss.insecure_tls = False
    ss.messages = []
    ss.info_timestamps = []
    ss.device_online = False
    ss.play_sound = False

    # Cargar datos previos
    try:
        if os.path.exists(DATA_FILE):
            df = pd.read_csv(DATA_FILE)
            ss.all_data_rows = df.to_dict('records')
            ss.diag.append(f"Cargados {len(ss.all_data_rows)} puntos desde {DATA_FILE}")
    except Exception as e:
        st.error(f"No se pudo cargar el archivo ({DATA_FILE}): {e}")

# =========================================================
# MQTT CALLBACKS
# =========================================================
def on_connect(client, userdata, flags, rc, properties=None):
    rc_value = rc.value
    ss.mqtt_connected = (rc_value == 0)
    rc_map = {0: "OK", 1: "Proto incorrecto", 2: "ID inválido", 3: "Servidor no disponible",
              4: "Usuario/Pass incorrecto", 5: "No autorizado"}
    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  on_connect rc={rc} ({rc_map.get(rc_value,'?')})")
    if rc_value == 0:
        client.subscribe([(T_STATE,1),(T_INFO,1),(T_LOGPART,1),(T_EVENTS,0)])
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Suscrito a tópicos.")

def on_disconnect(client, userdata, rc, properties=None):
    ss.mqtt_connected = False
    ss.device_online = False
    rc_value = rc.value if hasattr(rc, "value") else rc
    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  on_disconnect rc={rc_value}")

# =========================================================
# MQTT MENSAJES
# =========================================================
def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode("utf-8", errors="ignore")

        # --- INFO heartbeat ---
        if topic == T_INFO:
            now = time.time()
            ss.info_timestamps.append(now)
            ss.info_timestamps = ss.info_timestamps[-2:]
            if len(ss.info_timestamps) == 2 and not ss.device_online:
                if (ss.info_timestamps[1] - ss.info_timestamps[0]) < 11:
                    ss.device_online = True
                    ss.play_sound = True
                    ss.messages.append({"type":"success","text":"✅ ¡Conexión con la ESP32 establecida!"})
                    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  ESP32 online detectada.")

        # --- LOG PARTS (CHUNKS) ---
        elif topic == T_LOGPART:
            data = json.loads(payload)

            if not data.get("eof", False):
                # Guardamos texto del chunk
                ss.log_chunks.append(data.get("data", ""))
            else:
                # EOF → reconstruir CSV
                ss.download_in_progress = False
                ss.log_eof = True
                ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Log finalizado ({len(ss.log_chunks)} chunks).")

                if not ss.log_chunks:
                    ss.messages.append({"type":"warning","text":"⚠️ El dispositivo no envió datos."})
                    return

                # 1️⃣ Concatenar texto crudo (sin asumir líneas completas)
                full_bytes = "".join(ss.log_chunks)
                full_bytes = full_bytes.replace("\r\n", "\n").replace("\r", "\n")

                # 2️⃣ Filtrar líneas válidas (8 columnas separadas por comas)
                lines = full_bytes.split("\n")
                clean_lines = [ln for ln in lines if ln.strip() and ln.count(",") == 7]

                # 3️⃣ Insertar encabezado si falta
                if not clean_lines or not clean_lines[0].startswith("ts,"):
                    clean_lines.insert(0, "ts,lat,lon,alt,drop_id,speed_mps,sats,fix_ok")

                csv_text = "\n".join(clean_lines)

                # 4️⃣ Cargar CSV robustamente
                try:
                    csv_file = io.StringIO(csv_text)
                    df_new = pd.read_csv(csv_file, on_bad_lines='skip')
                except Exception as e:
                    ss.messages.append({"type":"error","text":f"Error al leer CSV: {e}"})
                    ss.diag.append(str(e))
                    return

                # 5️⃣ Guardar y actualizar sesión
                df_new.to_csv(DATA_FILE, index=False)
                ss.all_data_rows = df_new.to_dict("records")

                ss.messages.append({"type":"success","text":f"Log procesado ({len(df_new)} registros)."})
                st.toast(f"✅ ¡Log descargado y actualizado con {len(df_new)} registros!")

                ss.log_chunks.clear()
                ss.log_seq_last = -1

    except Exception as e:
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Error en on_message: {e}")
        ss.download_in_progress = False

# =========================================================
# FUNCIONES DE CONEXIÓN Y ENVÍO
# =========================================================
def connect_mqtt():
    if ss.mqtt_client: return
    try:
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Creando cliente MQTT...")
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             client_id=f"st-web-{int(time.time())}",
                             transport="websockets")
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        client.username_pw_set(BROKER_USER, BROKER_PASS)
        client.ws_set_options(path=BROKER_WS_PATH)
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.tls_insecure_set(bool(ss.insecure_tls))
        client.connect(BROKER_HOST, BROKER_PORT_WS, keepalive=60)
        ss.mqtt_client = client
    except Exception as e:
        ss.diag.append(f"Error al conectar MQTT: {e}")
        ss.mqtt_client = None

def disconnect_mqtt():
    if ss.mqtt_client:
        ss.mqtt_client.disconnect()
        ss.mqtt_client = None
        ss.mqtt_connected = False
        ss.device_online = False
        ss.diag.append("MQTT desconectado.")

def mqtt_publish(topic, payload_obj):
    if ss.mqtt_client and ss.mqtt_connected:
        try:
            ss.mqtt_client.publish(topic, json.dumps(payload_obj), qos=1)
            ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  CMD -> {payload_obj}")
            return True
        except Exception as e:
            ss.messages.append({"type":"error","text":f"Error al publicar: {e}"})
    else:
        ss.messages.append({"type":"warning","text":"Cliente MQTT no conectado."})
    return False

# =========================================================
# SIDEBAR
# =========================================================
login_box()
with st.sidebar:
    st.markdown("---")
    st.subheader("Conexión")
    if not ss.mqtt_client:
        if st.button("🔌 Conectar a MQTT", use_container_width=True):
            connect_mqtt(); st.rerun()
    else:
        if st.button("🔌 Desconectar", use_container_width=True, type="primary"):
            disconnect_mqtt(); st.rerun()

    st.subheader("Estado del Servidor")
    st.success("🟢 Conectado" if ss.mqtt_connected else "🔴 Desconectado")

    st.subheader("Estado del Dispositivo")
    if ss.device_online: st.success("✅ ESP32 Conectada")
    else: st.warning("⚪ Esperando ESP32...")

    ss.insecure_tls = st.checkbox("Usar TLS inseguro (debug)", value=ss.insecure_tls)

# =========================================================
# LOOP PRINCIPAL
# =========================================================
if ss.mqtt_client:
    ss.mqtt_client.loop(timeout=0.1)

if ss.play_sound:
    sound_html = f'<audio autoplay><source src="data:audio/wav;base64,{SUCCESS_SOUND_B64}" type="audio/wav"></audio>'
    st.components.v1.html(sound_html, height=0)
    ss.play_sound = False

# =========================================================
# INTERFAZ PRINCIPAL
# =========================================================
st.markdown("---")
left, mid, right = st.columns([1.4,1,1])
message_area = st.empty()

with left:
    st.subheader("Iniciar Misión")
    disabled = not is_editor()
    with st.form("start_form", clear_on_submit=False):
        velocity = st.number_input("Velocidad Drone (m/s)", 0.1, 100.0, 10.0, 0.1, disabled=disabled)
        distance = st.number_input("Distancia entre pelotas (m)", 0.1, 1000.0, 30.0, 0.1, disabled=disabled)
        delay_s  = st.number_input("Delay inicio (s)", 0.0, 120.0, 10.0, 1.0, disabled=disabled)
        step_hz  = st.number_input("Velocidad motor (Hz)", 1, 50000, 200, 10, disabled=disabled)
        if st.form_submit_button("🚀 Actualizar Parámetros", disabled=disabled, use_container_width=True):
            if is_editor():
                payload = {"action":"start","interval_s":float(distance/velocity),
                           "delay_s":float(delay_s),"step_hz":int(step_hz)}
                mqtt_publish(T_CMD, payload)
                ss.messages.append({"type":"success","text":"Parámetros actualizados y misión iniciada."})
            else:
                ss.messages.append({"type":"warning","text":"Necesitas ingresar el PIN."})
    if st.button("🚀 Inicio", type="primary", use_container_width=True):
        payload = {"action":"start","interval_s":float(distance/velocity),
                   "delay_s":float(delay_s),"step_hz":int(step_hz)}
        mqtt_publish(T_CMD, payload)
        ss.messages.append({"type":"info","text":"Comando Inicio enviado."})
        st.rerun()

with mid:
    st.subheader("Paro de emergencia")
    if st.button("⏹️ Paro Inmediato", type="primary", use_container_width=True):
        mqtt_publish(T_CMD, {"action":"stop"})
        ss.messages.append({"type":"info","text":"Comando STOP enviado."})
        st.rerun()

with right:
    st.subheader("Sincronizar Datos")
    if st.button("⬇️ Descargar Log Completo", use_container_width=True,
                 disabled=ss.get("download_in_progress", False)):
        ss.log_chunks = []
        ss.log_seq_last = -1
        ss.log_eof = False
        ss.download_in_progress = True
        mqtt_publish(T_CMD, {"action":"stream_log"})
        ss.messages.append({"type":"info","text":"Solicitud de log enviada. Esperando datos..."})
        st.rerun()

# =========================================================
# MENSAJES
# =========================================================
with message_area.container():
    if ss.download_in_progress:
        st.info("📥 Descargando y procesando el log, espere...")
    if ss.messages:
        for msg in ss.messages:
            getattr(st, msg["type"])(msg["text"])
        ss.messages = []
# =========================================================
# VISUALIZACIÓN DE DATOS (por defecto: día más reciente)
# =========================================================
st.markdown("---")
st.subheader("Historial de Ubicaciones")

df_all = pd.DataFrame(ss.all_data_rows) if ss.all_data_rows else pd.DataFrame(
    columns=["ts","lat","lon","alt","drop_id","speed_mps","sats","fix_ok"]
)

# 1) Si hay datos, crear columna 'dt' y calcular el día más reciente
if not df_all.empty:
    # tz-aware en MX para filtrar por fecha de manera natural
    df_all["dt"] = pd.to_datetime(df_all["ts"], unit="s", utc=True).dt.tz_convert("America/Mexico_City")
    latest_day = df_all["dt"].dt.date.max()  # <- día más reciente con datos
    default_day = latest_day
else:
    default_day = date.today()

# 2) El date_input ahora usa por defecto el día más reciente
day = st.date_input("Selecciona día", value=default_day, key="sel_day")
st.warning("_Si no ves datos, asegúrate de elegir la fecha correcta según el log._")
radius = st.slider("Radio de puntos (mapa)", 1, 50, 6, 1)

# 3) Filtrado por el día elegido
if not df_all.empty:
    day_start = pd.Timestamp.combine(day, datetime.min.time()).tz_localize("America/Mexico_City")
    day_end   = pd.Timestamp.combine(day, datetime.max.time()).tz_localize("America/Mexico_City")
    df_day = df_all[(df_all["dt"] >= day_start) & (df_all["dt"] <= day_end)].copy()
else:
    df_day = df_all

# 4) Métricas
m1,m2,m3,m4 = st.columns(4)
with m1: st.metric("Puntos (día)", len(df_day))
with m2: st.metric("Total puntos", len(ss.all_data_rows))
with m3: st.metric("GPS OK", int(df_day["fix_ok"].sum()) if not df_day.empty else 0)
with m4: st.metric("Velocidad Prom. (m/s)", f"{df_day['speed_mps'].mean():.2f}" if not df_day.empty else "—")

# 5) Mapa
if not df_day.empty and PYDECK_AVAILABLE:
    st.subheader("Mapa (día seleccionado)")
    df_map = df_day.dropna(subset=["lat","lon"])
    if not df_map.empty:
        st.pydeck_chart(pdk.Deck(
            initial_view_state=pdk.ViewState(latitude=float(df_map["lat"].mean()),
                                             longitude=float(df_map["lon"].mean()), zoom=15),
            layers=[pdk.Layer("ScatterplotLayer", data=df_map,
                              get_position='[lon, lat]', get_radius=radius,
                              pickable=True, get_fill_color='[255,0,0]')],
            tooltip={"text":"Drop #{drop_id}\n{dt}\nlat={lat}\nlon={lon}\nalt={alt} m\nspeed={speed_mps} m/s\nsats={sats}"}
        ))
elif not PYDECK_AVAILABLE:
    st.info("Pydeck no instalado → el mapa no se mostrará.")

# 6) Tabla
st.subheader("Tabla de Datos")
if not df_day.empty:
    st.dataframe(df_day.sort_values("ts", ascending=False), use_container_width=True, height=350)
else:
    if not df_all.empty and day != default_day:
        st.info("No hay datos para la fecha seleccionada. Prueba cambiando al día más reciente.")
    else:
        st.info("No hay datos para mostrar.")

# =========================================================
# AUTOREFRESCO
# =========================================================
time.sleep(1)
st.rerun()
