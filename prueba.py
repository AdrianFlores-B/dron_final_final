# Streamlit + HiveMQ Cloud — Versión Estable con Persistencia en CSV y Sin Hilos
import streamlit as st
import time, ssl, os, json
from datetime import datetime, date
import pandas as pd
import paho.mqtt.client as mqtt
import base64 ### MODIFICACIÓN ###: Importado para el sonido

# El archivo donde se guardarán los datos de forma permanente
DATA_FILE = "drone_data.csv"

# Pydeck es opcional si no se instala
try:
    import pydeck as pdk
    PYDECK_AVAILABLE = True
except ImportError:
    PYDECK_AVAILABLE = False

### MODIFICACIÓN ###: Sonido de éxito codificado para no necesitar archivos externos
SUCCESS_SOUND_B64 = "UklGRiQAAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YQAAAAA="

# ==============================================================================
# LÓGICA DE LA APLICACIÓN (ARQUITECTURA SIN HILOS)
# ==============================================================================

# ===================== SIMPLE LOGIN (PIN) =====================
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
                    st.session_state.auth_ok = True
                    st.success("Acceso concedido"); st.rerun()
                else: st.error("PIN incorrecto")
        else:
            st.success("Sesión: Editor")
            if st.button("Cerrar sesión", use_container_width=True):
                st.session_state.auth_ok = False; st.rerun()

# -------- HiveMQ Cloud creds (ACTUALIZA ESTOS VALORES) --------
BROKER_HOST    = "3f78afad5f2e407c85dd2eb93951af78.s1.eu.hivemq.cloud"
BROKER_PORT_WS = 8884
BROKER_WS_PATH = "/mqtt"
BROKER_USER    = "AdrianFB" # <-- Reemplaza
BROKER_PASS    = "Ab451278" # <-- Reemplaza

DEV_ID = "drone-001"
T_CMD      = f"drone/{DEV_ID}/cmd"
T_STATE    = f"drone/{DEV_ID}/state"
T_INFO     = f"drone/{DEV_ID}/info"
T_PREVIEW  = f"drone/{DEV_ID}/preview"
T_LOGPART  = f"drone/{DEV_ID}/log/part"
T_EVENTS   = f"drone/{DEV_ID}/events"

# -------- Interfaz y Lógica Principal de Streamlit --------
st.set_page_config(page_title="Ubicacion y Control de Avispas", layout="wide")
st.title("🛰️ Ubicacion y Control de Avispas")

ss = st.session_state

# Inicialización del estado de la sesión
if "init" not in ss:
    ss.init = True
    ss.mqtt_client = None
    ss.mqtt_connected = False
    ss.diag = []
    ss.preview_rows = []
    ss.log_chunks = []
    ss.log_seq_last = -1
    ss.log_eof = False
    ss.auth_ok = False
    ss.insecure_tls = False
    ss.messages = []
    
    ### MODIFICACIÓN ###: Nuevas variables para detectar la conexión de la ESP32
    ss.info_timestamps = [] # Guarda timestamps de mensajes en T_INFO
    ss.device_online = False   # Flag para saber si el dispositivo está conectado
    ss.play_sound = False      # Flag para reproducir el sonido una sola vez
    
    # --- Carga de datos persistentes y set para duplicados ---
    ss.seen_points = set()
    try:
        if os.path.exists(DATA_FILE):
            df = pd.read_csv(DATA_FILE)
            ss.preview_rows = df.to_dict('records')
            for row in ss.preview_rows:
                if all(k in row for k in ("ts", "lat", "lon")):
                    unique_tuple = (row['ts'], row['lat'], row['lon'])
                    ss.seen_points.add(unique_tuple)
            ss.diag.append(f"Cargados {len(ss.preview_rows)} puntos de datos desde {DATA_FILE}")
    except Exception as e:
        st.error(f"No se pudo cargar el archivo de datos ({DATA_FILE}): {e}")


# --- Callbacks de MQTT ---
def on_connect(client, userdata, flags, rc, properties=None):
    rc_value = rc.value
    ss.mqtt_connected = (rc_value == 0)
    rc_map = {0: "OK", 1: "Proto incorrecto", 2: "ID cliente inválido", 3: "Servidor no disponible", 4: "Usuario/Pass incorrecto", 5: "No autorizado"}
    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  on_connect rc={rc} ({rc_map.get(rc_value, 'Desconocido')})")
    if rc_value == 0:
        client.subscribe([(T_STATE,1),(T_INFO,1),(T_PREVIEW,1),(T_LOGPART,1),(T_EVENTS,0)])
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Suscrito a todos los tópicos.")

def on_disconnect(client, userdata, rc, properties=None):
    ss.mqtt_connected = False
    ### MODIFICACIÓN ###: Reiniciar el estado del dispositivo al desconectar
    ss.device_online = False
    if hasattr(rc, 'value'): rc_value = rc.value
    else: rc_value = rc
    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  on_disconnect rc={rc_value}")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode("utf-8", errors="ignore")
        
        ### MODIFICACIÓN ###: Lógica para detectar si la ESP32 está online
        if topic == T_INFO:
            now = time.time()
            ss.info_timestamps.append(now)
            
            # Mantenemos solo los últimos 2 timestamps para no llenar la memoria
            if len(ss.info_timestamps) > 2:
                ss.info_timestamps = ss.info_timestamps[-2:]
            
            # Si tenemos 2 mensajes y no hemos confirmado la conexión...
            if len(ss.info_timestamps) == 2 and not ss.device_online:
                time_diff = ss.info_timestamps[1] - ss.info_timestamps[0]
                
                # Si la diferencia es menor a 11 segundos, consideramos que está conectada
                if time_diff < 11:
                    ss.device_online = True
                    ss.play_sound = True # Activa la bandera para reproducir sonido
                    ss.messages.append({"type": "success", "text": "✅ ¡Conexión con la ESP32 establecida!"})
                    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  ESP32 detectada online.")
        
        elif topic == T_PREVIEW:
            data = json.loads(payload)
            if isinstance(data, list) and data:
                new_rows = []
                for point in data:
                    if all(k in point for k in ("ts", "lat", "lon")):
                        unique_tuple = (point['ts'], point['lat'], point['lon'])
                        if unique_tuple not in ss.seen_points:
                            new_rows.append(point)
                            ss.seen_points.add(unique_tuple)
                
                if new_rows:
                    ss.preview_rows.extend(new_rows)
                    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Recibidas y guardadas {len(new_rows)} filas nuevas.")
                    st.toast(f"🛰️ ¡Se guardaron {len(new_rows)} nuevos puntos de datos!")
                    
                    new_df = pd.DataFrame(new_rows)
                    header = not os.path.exists(DATA_FILE)
                    new_df.to_csv(DATA_FILE, mode='a', header=header, index=False)
                else:
                    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Datos recibidos eran duplicados.")
    except Exception as e:
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Error en on_message: {e}")

# --- Funciones de Conexión y Publicación ---
def connect_mqtt():
    if ss.mqtt_client: return
    try:
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Creando cliente MQTT...")
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"st-web-{int(time.time())}", transport="websockets")
        client.on_connect = on_connect; client.on_disconnect = on_disconnect; client.on_message = on_message
        client.username_pw_set(BROKER_USER, BROKER_PASS)
        client.ws_set_options(path=BROKER_WS_PATH)
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED); client.tls_insecure_set(bool(ss.insecure_tls))
        client.connect(BROKER_HOST, BROKER_PORT_WS, keepalive=60)
        ss.mqtt_client = client
    except Exception as e:
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  ERROR AL CONECTAR: {e}")
        ss.mqtt_client = None

def disconnect_mqtt():
    if ss.mqtt_client:
        ss.mqtt_client.disconnect()
        ss.mqtt_client = None; ss.mqtt_connected = False
        ### MODIFICACIÓN ###: Reiniciar el estado del dispositivo
        ss.device_online = False
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Cliente desconectado.")

def mqtt_publish(topic, payload_obj):
    if ss.mqtt_client and ss.mqtt_connected:
        try:
            ss.mqtt_client.publish(topic, json.dumps(payload_obj), qos=1)
            ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  CMD -> {payload_obj}")
            return True
        except Exception as e:
            ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Error al publicar: {e}")
            ss.messages.append({"type": "error", "text": f"Error al publicar: {e}"})
            return False
    else:
        ss.messages.append({"type": "warning", "text": "No se puede publicar, cliente no conectado."})
        return False

# --- Barra Lateral (UI) ---
login_box()
with st.sidebar:
    st.markdown("---"); st.subheader("Conexión")
    if not ss.mqtt_client:
        if st.button("🔌 Conectar a MQTT", use_container_width=True):
            connect_mqtt(); st.rerun()
    else:
        if st.button("🔌 Desconectar", use_container_width=True, type="primary"):
            disconnect_mqtt(); st.rerun()
            
    st.subheader("Estado del Servidor")
    if ss.mqtt_connected: st.success("🟢 Conectado")
    else: st.error("🔴 Desconectado")

    ### MODIFICACIÓN ###: Indicador de estado para la ESP32
    st.subheader("Estado del Dispositivo")
    if ss.get("device_online", False):
        st.success("✅ ESP32 Conectada")
    else:
        st.warning("⚪ Esperando ESP32...")

    st.subheader("Opciones")
    ss.insecure_tls = st.checkbox("Usar TLS Inseguro (Debug)", value=ss.insecure_tls)

# --- Bucle Principal / "Tick" ---
if ss.mqtt_client:
    ss.mqtt_client.loop(timeout=0.1)

### MODIFICACIÓN ###: Lógica para reproducir el sonido
if ss.get("play_sound", False):
    sound_html = f"""
    <audio autoplay>
      <source src="data:audio/wav;base64,{SUCCESS_SOUND_B64}" type="audio/wav">
    </audio>
    """
    st.components.v1.html(sound_html, height=0)
    ss.play_sound = False # Se resetea para que no suene en cada rerun

# -------- Controles Principales --------
st.markdown("---")
left, mid, right = st.columns([1.4,1,1])
message_area = st.empty()

with left:
    st.subheader("Iniciar Mision")
    
    disabled = not is_editor()
    with st.form("start_form", clear_on_submit=False):
        velocity = st.number_input("Velocidad Drone (m/s)", 0.1, 100.0, 10.0, 0.1, disabled=disabled)
        distance = st.number_input("Distancia entre pelotas (m)", 0.1, 1000.0, 30.0, 0.1, disabled=disabled)
        delay_s  = st.number_input("Delay de inicio (s)", 0.0, 120.0, 10.0, 1.0, disabled=disabled)
        step_hz  = st.number_input("Velocidad motor (200 - 1500)", 1, 50000, 200, 10, disabled=disabled)
        try:
            st.info(f"Intervalo calculado: **{distance/velocity:.2f} s**" if velocity > 0 else "Intervalo: —")
        except Exception: st.info("Intervalo: —")
        
        start_clicked = st.form_submit_button("🚀 Actualizar Parámetros", disabled=disabled, use_container_width=True)
        if start_clicked:
            if is_editor():
                payload = {"action":"start", "interval_s": float(distance/velocity), "delay_s": float(delay_s), "step_hz": int(step_hz)}
                if mqtt_publish(T_CMD, payload):
                    ss.messages.append({"type": "success", "text": "Comando de INICIO enviado con nuevos parámetros."})
            else: 
                ss.messages.append({"type": "warning", "text": "Necesitas ingresar el PIN para iniciar la misión."})
    inicio = st.button("🚀 Inicio", type="primary", use_container_width=True)
    if inicio:
        payload = {"action":"start", "interval_s": float(distance/velocity), "delay_s": float(delay_s), "step_hz": int(step_hz)}
        if mqtt_publish(T_CMD, payload):
            ss.messages.append({"type": "info", "text": "Comando Inicio enviado."})
            st.rerun()
with mid:
    st.subheader("Paro de emergencia")
    if st.button("⏹️ Paro Inmediato", type="primary", use_container_width=True):
        if mqtt_publish(T_CMD, {"action":"stop"}):
            ss.messages.append({"type": "info", "text": "Comando STOP enviado."})
            st.rerun()

with right:
    st.subheader("Solicitar Datos")
    pvN = st.number_input("Puntos de preview:", 1, 2000, 50, 50)
    if st.button("📥 Solicitar Preview", use_container_width=True):
        if mqtt_publish(T_CMD, {"action":"preview", "last": int(pvN)}):
            ss.messages.append({"type": "info", "text": "Solicitud de preview enviada."})
            st.rerun()
    if st.button("⬇️ Descargar Log Completo", use_container_width=True):
        ss.log_chunks = []; ss.log_seq_last = -1; ss.log_eof = False
        if mqtt_publish(T_CMD, {"action":"stream_log"}):
            ss.messages.append({"type": "info", "text": "Solicitud de log completo enviada."})
            st.rerun()

# --- Lógica para mostrar mensajes ---
with message_area.container():
    if "messages" in ss and ss.messages:
        for msg in ss.messages:
            if msg["type"] == "success": st.success(msg["text"])
            elif msg["type"] == "info": st.info(msg["text"])
            elif msg["type"] == "warning": st.warning(msg["text"])
            elif msg["type"] == "error": st.error(msg["text"])
        ss.messages = [] # Limpiar mensajes después de mostrarlos

# -------- Visualización de Datos (Mapa y Tabla) --------
st.markdown("---")
st.subheader("Historial de Ubicaciones")

df_all = pd.DataFrame(ss.preview_rows) if ss.preview_rows else pd.DataFrame(
    columns=["ts","lat","lon","alt","drop_id","speed_mps","sats","fix_ok"]
)

day = st.date_input("Escoge un dia", value=date.today())
st.warning("_**Nota:** Si no ves datos, ¡asegúrate de que la fecha seleccionada sea la correcta!_")
radius = st.slider("Radio de los puntos (mapa)", 1, 50, 6, 1)

if not df_all.empty:
    df_all["dt"] = pd.to_datetime(df_all["ts"], unit="s", utc=True).dt.tz_convert("America/Mexico_City")
    day_start = pd.Timestamp.combine(day, datetime.min.time()).tz_localize("America/Mexico_City")
    day_end   = pd.Timestamp.combine(day, datetime.max.time()).tz_localize("America/Mexico_City")
    df_day = df_all[(df_all["dt"] >= day_start) & (df_all["dt"] <= day_end)].copy()
else:
    df_day = df_all

m1, m2, m3, m4 = st.columns(4)
with m1: st.metric("Puntos (día seleccionado)", len(df_day))
with m2: st.metric("Total de puntos guardados", len(ss.preview_rows))
with m3: st.metric("Puntos con GPS OK", int(df_day["fix_ok"].sum()) if not df_day.empty and "fix_ok" in df_day else 0)
with m4: st.metric("Velocidad Prom. (m/s)", f"{df_day['speed_mps'].mean():.2f}" if not df_day.empty and "speed_mps" in df_day else "—")

if not df_day.empty and PYDECK_AVAILABLE:
    st.subheader("Mapa (día seleccionado)")
    df_map = df_day.dropna(subset=['lat', 'lon'])
    if not df_map.empty:
        lat0 = float(df_map["lat"].mean())
        lon0 = float(df_map["lon"].mean())
        st.pydeck_chart(pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(latitude=lat0, longitude=lon0, zoom=15),
            layers=[pdk.Layer("ScatterplotLayer", data=df_map,
                              get_position='[lon, lat]', get_radius=radius,
                              pickable=True, get_fill_color='[255,0,0]')],
            tooltip={"text": "Drop #{drop_id}\n{dt}\nlat={lat}\nlon={lon}\nalt={alt} m\nspeed={speed_mps} m/s\nsats={sats}\nfix_ok={fix_ok}"},
        ))
elif not PYDECK_AVAILABLE:
    st.info("La librería Pydeck no está instalada. El mapa no puede mostrarse.")

st.subheader("Tabla de Datos (día seleccionado)")
if not df_day.empty:
    st.dataframe(df_day.sort_values("ts", ascending=False), use_container_width=True, height=350)
else:
    st.info("No hay datos para mostrar en la fecha seleccionada.")


# --- Auto-refresco ---
time.sleep(1)
st.rerun()
