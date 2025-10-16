# Streamlit + HiveMQ Cloud — Versión con Descarga de Log Completo (Robusta)
import streamlit as st
import time, ssl, os, json
from datetime import datetime, date
import pandas as pd
import paho.mqtt.client as mqtt
import base64
import io

# El archivo donde se guardarán los datos de forma permanente
DATA_FILE = "drone_data.csv"

# Pydeck es opcional si no se instala
try:
    import pydeck as pdk
    PYDECK_AVAILABLE = True
except ImportError:
    PYDECK_AVAILABLE = False

# Sonido de éxito codificado para no necesitar archivos externos
SUCCESS_SOUND_B64 = "UklGRiQAAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YQAAAAA="

# ==============================================================================
# LÓGICA DE LA APLICACIÓN
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

# -------- HiveMQ Cloud creds --------
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
    ss.all_data_rows = []
    ss.log_chunks = []
    ss.download_in_progress = False
    ss.auth_ok = False
    ss.insecure_tls = False
    ss.messages = []
    ss.info_timestamps = []
    ss.device_online = False
    ss.play_sound = False
    
    try:
        if os.path.exists(DATA_FILE):
            df = pd.read_csv(DATA_FILE)
            ss.all_data_rows = df.to_dict('records')
            ss.diag.append(f"Cargados {len(ss.all_data_rows)} puntos de datos desde {DATA_FILE}")
    except Exception as e:
        st.error(f"No se pudo cargar el archivo de datos ({DATA_FILE}): {e}")

# --- Callbacks de MQTT ---
def on_connect(client, userdata, flags, rc, properties=None):
    rc_value = rc.value
    ss.mqtt_connected = (rc_value == 0)
    rc_map = {0: "OK", 1: "Proto incorrecto", 2: "ID cliente inválido", 3: "Servidor no disponible", 4: "Usuario/Pass incorrecto", 5: "No autorizado"}
    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  on_connect rc={rc} ({rc_map.get(rc_value, 'Desconocido')})")
    if rc_value == 0:
        client.subscribe([(T_STATE,1),(T_INFO,1),(T_LOGPART,1),(T_EVENTS,0)])
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Suscrito a tópicos principales.")

def on_disconnect(client, userdata, rc, properties=None):
    ss.mqtt_connected = False
    ss.device_online = False
    if hasattr(rc, 'value'): rc_value = rc.value
    else: rc_value = rc
    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  on_disconnect rc={rc_value}")

### MODIFICACIÓN ###: Función on_message completamente reescrita para ser más robusta
def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode("utf-8", errors="ignore")

        if topic == T_INFO:
            now = time.time()
            ss.info_timestamps.append(now)
            if len(ss.info_timestamps) > 2: ss.info_timestamps = ss.info_timestamps[-2:]
            
            if len(ss.info_timestamps) == 2 and not ss.device_online:
                if (ss.info_timestamps[1] - ss.info_timestamps[0]) < 11:
                    ss.device_online = True
                    ss.play_sound = True
                    ss.messages.append({"type": "success", "text": "✅ ¡Conexión con la ESP32 establecida!"})
        
        elif topic == T_LOGPART:
            try:
                data = json.loads(payload)
                
                if not data.get('eof', False):
                    # Es un chunk de datos, lo guardamos si tiene contenido
                    if 'data' in data and 'seq' in data:
                        ss.log_chunks.append(data)
                else:
                    # Mensaje de Fin de Archivo (EOF). Procesamos los chunks acumulados.
                    ss.diag.append(f"EOF recibido. Procesando {len(ss.log_chunks)} chunks.")
                    try:
                        if not ss.log_chunks:
                            ss.messages.append({"type": "warning", "text": "Descarga completada, pero el log estaba vacío."})
                        else:
                            # Ordenamos y unimos todos los datos en un solo string
                            ss.log_chunks.sort(key=lambda x: x['seq'])
                            full_csv_string = "".join(chunk['data'] for chunk in ss.log_chunks)

                            if not full_csv_string.strip():
                                ss.messages.append({"type": "warning", "text": "El log no contenía registros de datos."})
                            else:
                                # Leemos el string como si fuera un archivo CSV
                                csv_file = io.StringIO(full_csv_string)
                                df_new = pd.read_csv(csv_file)
                                
                                # Guardamos el archivo y actualizamos la app
                                df_new.to_csv(DATA_FILE, index=False)
                                ss.all_data_rows = df_new.to_dict('records')
                                
                                ss.messages.append({"type": "success", "text": f"Log procesado. Se cargaron {len(df_new)} registros."})
                                st.toast(f"✅ Log actualizado: {len(df_new)} registros.")
                    
                    except pd.errors.EmptyDataError:
                        ss.messages.append({"type": "error", "text": "Error al procesar: El log estaba vacío o mal formado."})
                    except Exception as e:
                        ss.messages.append({"type": "error", "text": f"Error al procesar los datos del log: {e}"})
                        ss.diag.append(f"Error durante el procesamiento del CSV: {e}")
                    finally:
                        # ESTE BLOQUE ES CRÍTICO: Se ejecuta siempre (con éxito o error)
                        # para limpiar el estado y rehabilitar el botón.
                        ss.log_chunks = []
                        ss.download_in_progress = False

            except json.JSONDecodeError:
                ss.messages.append({"type": "error", "text": "Error de comunicación: Se recibió un dato corrupto."})
                ss.diag.append(f"JSONDecodeError en payload: {payload}")
                ss.download_in_progress = False
                ss.log_chunks = []

    except Exception as e:
        ss.diag.append(f"Error mayor en on_message: {e}")
        ss.messages.append({"type": "error", "text": "Ocurrió un error inesperado en la comunicación."})
        if 'download_in_progress' in ss:
            ss.download_in_progress = False
        if 'log_chunks' in ss:
            ss.log_chunks = []


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

if ss.get("play_sound", False):
    sound_html = f'<audio autoplay><source src="data:audio/wav;base64,{SUCCESS_SOUND_B64}" type="audio/wav"></audio>'
    st.components.v1.html(sound_html, height=0)
    ss.play_sound = False

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
        
        if st.form_submit_button("🚀 Actualizar Parámetros", disabled=disabled, use_container_width=True):
            if is_editor():
                payload = {"action":"start", "interval_s": float(distance/velocity), "delay_s": float(delay_s), "step_hz": int(step_hz)}
                if mqtt_publish(T_CMD, payload):
                    ss.messages.append({"type": "success", "text": "Comando de INICIO enviado con nuevos parámetros."})
            else: 
                ss.messages.append({"type": "warning", "text": "Necesitas ingresar el PIN para iniciar la misión."})
    if st.button("🚀 Inicio", type="primary", use_container_width=True):
        payload = {"action":"start", "interval_s": float(distance/velocity), "delay_s": float(delay_s), "step_hz": int(step_hz)}
        if mqtt_publish(T_CMD, payload):
            ss.messages.append({"type": "info", "text": "Comando Inicio enviado."}); st.rerun()

with mid:
    st.subheader("Paro de emergencia")
    if st.button("⏹️ Paro Inmediato", type="primary", use_container_width=True):
        if mqtt_publish(T_CMD, {"action":"stop"}):
            ss.messages.append({"type": "info", "text": "Comando STOP enviado."}); st.rerun()

with right:
    st.subheader("Sincronizar Datos")
    if st.button("⬇️ Descargar Log Completo", use_container_width=True, disabled=ss.get("download_in_progress", False)):
        ss.log_chunks = []
        ss.download_in_progress = True
        if mqtt_publish(T_CMD, {"action":"stream_log"}):
            ss.messages.append({"type": "info", "text": "Solicitud de log enviada. Recibiendo datos..."})
            st.rerun()

# --- Lógica para mostrar mensajes ---
with message_area.container():
    if ss.get("download_in_progress", False):
        st.info("📥 Descargando y procesando el log del dispositivo. Por favor, espere...")
    
    if "messages" in ss and ss.messages:
        for msg in ss.messages:
            if msg["type"] == "success": st.success(msg["text"])
            elif msg["type"] == "info": st.info(msg["text"])
            elif msg["type"] == "warning": st.warning(msg["text"])
            elif msg["type"] == "error": st.error(msg["text"])
        ss.messages = []

# -------- Visualización de Datos (Mapa y Tabla) --------
st.markdown("---")
st.subheader("Historial de Ubicaciones")

df_all = pd.DataFrame(ss.all_data_rows) if ss.all_data_rows else pd.DataFrame(
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
with m2: st.metric("Total de puntos guardados", len(ss.all_data_rows))
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
