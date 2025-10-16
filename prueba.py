# =========================================================
# Streamlit + HiveMQ Cloud — Versión robusta y bloqueante
# - Sin threads
# - Descarga de log en modo bloqueante (timeout 16s)
# - Mantiene loop() en idle para no perder conexión
# =========================================================

import streamlit as st
import time, ssl, os, json, io, base64
from datetime import datetime, date
import pandas as pd
import paho.mqtt.client as mqtt

# ---------- Archivo de datos persistente ----------
DATA_FILE = "drone_data.csv"

# ---------- Pydeck opcional ----------
try:
    import pydeck as pdk
    PYDECK_AVAILABLE = True
except ImportError:
    PYDECK_AVAILABLE = False

# ---------- Sonido tiny WAV ----------
SUCCESS_SOUND_B64 = "UklGRiQAAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YQAAAAA="

# =========================================================
# LOGIN simple por PIN
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
            if st.button("Ingresar", width="stretch"):
                if pin_try and pin_try.strip() == APP_PIN:
                    st.session_state.auth_ok = True
                    st.success("Acceso concedido")
                    st.rerun()
                else:
                    st.error("PIN incorrecto")
        else:
            st.success("Sesión: Editor")
            if st.button("Cerrar sesión", width="stretch"):
                st.session_state.auth_ok = False
                st.rerun()

# =========================================================
# MQTT CONFIG (HiveMQ Cloud, WebSockets TLS)
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
# Estado de sesión
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

    # Buffer de descarga
    ss.log_chunks = []          # lista de strings data
    ss.log_eof = False          # bandera EOF recibido
    ss.log_collecting = False   # estamos en descarga bloqueante
    ss.last_chunk_at = 0.0      # timestamp del último chunk recibido

    ss.auth_ok = False
    ss.insecure_tls = False
    ss.messages = []
    ss.info_timestamps = []
    ss.device_online = False
    ss.play_sound = False

    # Carga de datos previos
    try:
        if os.path.exists(DATA_FILE):
            df = pd.read_csv(DATA_FILE)
            ss.all_data_rows = df.to_dict('records')
            ss.diag.append(f"Cargados {len(ss.all_data_rows)} puntos desde {DATA_FILE}")
    except Exception as e:
        st.error(f"No se pudo cargar el archivo ({DATA_FILE}): {e}")

# =========================================================
# MQTT Callbacks
# =========================================================
def on_connect(client, userdata, flags, rc, properties=None):
    # paho v2 envía un enum con .value; soportamos ambos
    rc_value = rc.value if hasattr(rc, "value") else rc
    ss.mqtt_connected = (rc_value == 0)
    rc_map = {0: "OK", 1: "Proto incorrecto", 2: "ID inválido", 3: "Servidor no disponible",
              4: "Usuario/Pass incorrecto", 5: "No autorizado"}
    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')} on_connect rc={rc_value} ({rc_map.get(rc_value,'?')})")
    if rc_value == 0:
        client.subscribe([(T_STATE,1),(T_INFO,1),(T_LOGPART,1),(T_EVENTS,0)])
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')} Suscrito a tópicos.")

def on_disconnect(client, userdata, rc, properties=None):
    rc_value = rc.value if hasattr(rc, "value") else rc
    ss.mqtt_connected = False
    ss.device_online = False
    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')} on_disconnect rc={rc_value}")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode("utf-8", errors="ignore")

        if topic == T_INFO:
            now = time.time()
            ss.info_timestamps.append(now)
            ss.info_timestamps = ss.info_timestamps[-2:]
            if len(ss.info_timestamps) == 2 and not ss.device_online:
                if (ss.info_timestamps[1] - ss.info_timestamps[0]) < 11:
                    ss.device_online = True
                    ss.play_sound = True
                    ss.messages.append({"type":"success","text":"✅ ¡Conexión con la ESP32 establecida!"})
                    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')} ESP32 online detectada.")

        elif topic == T_LOGPART:
            data = json.loads(payload)
            # registramos momento de recepción (para watchdogs si quieres)
            ss.last_chunk_at = time.time()

            if not data.get("eof", False):
                # acumulamos solo la parte 'data'
                # respetamos posibles \r\n y fragmentos
                chunk_text = data.get("data", "")
                if chunk_text:
                    ss.log_chunks.append(chunk_text)
            else:
                # EOF: marcamos fin
                ss.log_eof = True

    except Exception as e:
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')} Error en on_message: {e}")

# =========================================================
# Conexión y publicación
# =========================================================
def connect_mqtt():
    if ss.mqtt_client:
        return
    try:
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')} Creando cliente MQTT...")
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             client_id=f"st-web-{int(time.time())}",
                             transport="websockets")
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        client.username_pw_set(BROKER_USER, BROKER_PASS)
        client.ws_set_options(path=BROKER_WS_PATH)

        # TLS configuración (por defecto: verificado). Si no tienes CA, puedes activar inseguro en el sidebar.
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.tls_insecure_set(bool(ss.insecure_tls))

        client.connect(BROKER_HOST, BROKER_PORT_WS, keepalive=60)
        ss.mqtt_client = client

        # Espera activa hasta 2s por el CONNACK para establecer ss.mqtt_connected
        t0 = time.time()
        while not ss.mqtt_connected and time.time() - t0 < 2.0:
            ss.mqtt_client.loop(timeout=0.1)

    except Exception as e:
        ss.diag.append(f"Error al conectar MQTT: {e}")
        ss.mqtt_client = None

def disconnect_mqtt():
    if ss.mqtt_client:
        try:
            ss.mqtt_client.disconnect()
        except Exception:
            pass
        ss.mqtt_client = None
        ss.mqtt_connected = False
        ss.device_online = False
        ss.diag.append("MQTT desconectado.")

def mqtt_publish(topic, payload_obj):
    if ss.mqtt_client and ss.mqtt_connected:
        try:
            ss.mqtt_client.publish(topic, json.dumps(payload_obj), qos=1)
            ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')} CMD -> {payload_obj}")
            return True
        except Exception as e:
            ss.messages.append({"type":"error","text":f"Error al publicar: {e}"})
    else:
        ss.messages.append({"type":"warning","text":"Cliente MQTT no conectado."})
    return False

# =========================================================
# Descarga bloqueante del log (sin rerun, timeout 16s)
# =========================================================
def fetch_log_blocking(timeout_s=16.0):
    """
    - Limpia buffers
    - Publica stream_log
    - Bombea loop() hasta que llegue EOF o timeout
    - Reconstruye y guarda CSV
    - Devuelve (ok, n_registros, msg_error)
    """
    if not ss.mqtt_client or not ss.mqtt_connected:
        return (False, 0, "MQTT no conectado")

    # Preparar estado
    ss.log_chunks = []
    ss.log_eof = False
    ss.last_chunk_at = time.time()
    ss.log_collecting = True  # <- evita reruns en idle loop

    # Solicitar log
    ok_pub = mqtt_publish(T_CMD, {"action": "stream_log"})
    if not ok_pub:
        ss.log_collecting = False
        return (False, 0, "No se pudo publicar stream_log")

    t0 = time.time()
    # Bucle de recepción: solo loop() y chequeos; SIN st.rerun aquí
    while True:
        # Bombeo intensivo
        ss.mqtt_client.loop(timeout=0.1)

        # Fin por EOF
        if ss.log_eof:
            break

        # Timeout total
        if time.time() - t0 > timeout_s:
            break

    # Terminamos la fase bloqueante
    ss.log_collecting = False

    # Si no llegó EOF, reportamos timeout
    if not ss.log_eof:
        return (False, 0, f"Timeout: no llegó EOF en {timeout_s:.0f}s (chunks={len(ss.log_chunks)})")

    # Reconstruir CSV
    try:
        raw_text = "".join(ss.log_chunks)
        raw_text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
        lines = raw_text.split("\n")
        # Mantener solo líneas con 7 comas (8 columnas)
        clean_lines = [ln for ln in lines if ln.strip() and ln.count(",") == 7]

        # Insertar encabezado si no está
        if not clean_lines or not clean_lines[0].startswith("ts,"):
            clean_lines.insert(0, "ts,lat,lon,alt,drop_id,speed_mps,sats,fix_ok")

        csv_text = "\n".join(clean_lines)
        df_new = pd.read_csv(io.StringIO(csv_text), on_bad_lines='skip')

        # Guardar y actualizar sesión
        df_new.to_csv(DATA_FILE, index=False)
        ss.all_data_rows = df_new.to_dict("records")
        return (True, len(df_new), "")
    except Exception as e:
        return (False, 0, f"Error al procesar CSV: {e}")

# =========================================================
# Sidebar
# =========================================================
login_box()
with st.sidebar:
    st.markdown("---")
    st.subheader("Conexión")
    if not ss.mqtt_client:
        if st.button("🔌 Conectar a MQTT", width="stretch"):
            connect_mqtt()
            st.rerun()
    else:
        if st.button("🔌 Desconectar", width="stretch", type="primary"):
            disconnect_mqtt()
            st.rerun()

    st.subheader("Estado del Servidor")
    st.success("🟢 Conectado" if ss.mqtt_connected else "🔴 Desconectado")

    st.subheader("Estado del Dispositivo")
    if ss.device_online:
        st.success("✅ ESP32 Conectada")
    else:
        st.warning("⚪ Esperando ESP32...")

    ss.insecure_tls = st.checkbox("Usar TLS inseguro (debug)", value=ss.insecure_tls,
                                  help="Solo si tu sistema no tiene CA. No usar en producción.")

# =========================================================
# Campanita al conectar
# =========================================================
if ss.play_sound:
    sound_html = f'<audio autoplay><source src="data:audio/wav;base64,{SUCCESS_SOUND_B64}" type="audio/wav"></audio>'
    st.components.v1.html(sound_html, height=0)
    ss.play_sound = False

# =========================================================
# UI principal
# =========================================================
st.markdown("---")
left, mid, right = st.columns([1.4, 1, 1])
message_area = st.empty()

with left:
    st.subheader("Iniciar Misión")
    disabled = not is_editor()
    with st.form("start_form", clear_on_submit=False):
        velocity = st.number_input("Velocidad Drone (m/s)", 0.1, 100.0, 10.0, 0.1, disabled=disabled)
        distance = st.number_input("Distancia entre pelotas (m)", 0.1, 1000.0, 30.0, 0.1, disabled=disabled)
        delay_s  = st.number_input("Delay inicio (s)", 0.0, 120.0, 10.0, 1.0, disabled=disabled)
        step_hz  = st.number_input("Velocidad motor (Hz)", 1, 50000, 200, 10, disabled=disabled)

        if st.form_submit_button("🚀 Actualizar Parámetros", disabled=disabled, width="stretch"):
            if is_editor():
                payload = {"action":"start",
                           "interval_s": float(distance/velocity),
                           "delay_s": float(delay_s),
                           "step_hz": int(step_hz)}
                mqtt_publish(T_CMD, payload)
                ss.messages.append({"type":"success","text":"Parámetros enviados y misión armada/iniciada."})
            else:
                ss.messages.append({"type":"warning","text":"Necesitas ingresar el PIN."})

    if st.button("🚀 Inicio", type="primary", width="stretch"):
        payload = {"action":"start",
                   "interval_s": float(distance/velocity),
                   "delay_s": float(delay_s),
                   "step_hz": int(step_hz)}
        mqtt_publish(T_CMD, payload)
        ss.messages.append({"type":"info","text":"Comando Inicio enviado."})
        st.rerun()

with mid:
    st.subheader("Paro de emergencia")
    if st.button("⏹️ Paro Inmediato", type="primary", width="stretch"):
        mqtt_publish(T_CMD, {"action":"stop"})
        ss.messages.append({"type":"info","text":"Comando STOP enviado."})
        st.rerun()

with right:
    st.subheader("Sincronizar Datos")
    # Descarga bloqueante: NO provoca rerun hasta terminar o timeout
    if st.button("⬇️ Descargar Log Completo", width="stretch",
                 disabled=ss.log_collecting):
        ss.messages.append({"type":"info","text":"Solicitud de log enviada. Recibiendo datos..."})
        ok, n, err = fetch_log_blocking(timeout_s=16.0)
        if ok:
            ss.messages.append({"type":"success","text":f"Log procesado ({n} registros)."})
            st.toast(f"✅ ¡Log descargado y actualizado con {n} registros!")
        else:
            ss.messages.append({"type":"error","text":f"No se pudo completar la descarga: {err}"})
        # tras terminar (o fallar), hacemos un rerun para refrescar la UI
        st.rerun()

# =========================================================
# Mensajes
# =========================================================
with message_area.container():
    if ss.log_collecting:
        st.info("📥 Descargando y procesando el log, espere...")
    for msg in ss.messages:
        getattr(st, msg["type"])(msg["text"])
    ss.messages.clear()

# =========================================================
# Visualización (por defecto: día más reciente disponible)
# =========================================================
st.markdown("---")
st.subheader("Historial de Ubicaciones")

df_all = pd.DataFrame(ss.all_data_rows) if ss.all_data_rows else pd.DataFrame(
    columns=["ts","lat","lon","alt","drop_id","speed_mps","sats","fix_ok"]
)

# Construimos 'dt' y detectamos el día por defecto de forma segura
if not df_all.empty:
    # Convertir tipos por si hay strings raras
    for col, typ in [("ts","float"), ("lat","float"), ("lon","float"),
                     ("alt","float"), ("drop_id","float"),
                     ("speed_mps","float"), ("sats","float"), ("fix_ok","float")]:
        if col in df_all.columns:
            df_all[col] = pd.to_numeric(df_all[col], errors="coerce")

    df_all["dt"] = pd.to_datetime(df_all["ts"], unit="s", errors="coerce", utc=True)
    df_all["dt"] = df_all["dt"].dt.tz_convert("America/Mexico_City")

    if df_all["dt"].notna().any():
        latest_day = df_all.loc[df_all["dt"].notna(), "dt"].dt.date.max()
        default_day = latest_day if isinstance(latest_day, date) else date.today()
    else:
        default_day = date.today()
else:
    default_day = date.today()

day = st.date_input("Selecciona día", value=default_day, key="sel_day")
st.caption("_Si no ves datos, asegúrate de elegir la fecha correcta según el log._")
radius = st.slider("Radio de puntos (mapa)", 1, 50, 6, 1)

# Filtrado del día
if not df_all.empty and "dt" in df_all.columns:
    day_start = pd.Timestamp.combine(day, datetime.min.time()).tz_localize("America/Mexico_City")
    day_end   = pd.Timestamp.combine(day, datetime.max.time()).tz_localize("America/Mexico_City")
    df_day = df_all[(df_all["dt"] >= day_start) & (df_all["dt"] <= day_end)].copy()
else:
    df_day = pd.DataFrame(columns=df_all.columns)

# Métricas
m1, m2, m3, m4 = st.columns(4)
with m1: st.metric("Puntos (día)", len(df_day))
with m2: st.metric("Total puntos", len(ss.all_data_rows))
with m3: st.metric("GPS OK", int(df_day["fix_ok"].fillna(0).sum()) if not df_day.empty else 0)
with m4: st.metric("Velocidad Prom. (m/s)",
                   f"{df_day['speed_mps'].mean():.2f}" if not df_day.empty else "—")

# Mapa
if not df_day.empty and PYDECK_AVAILABLE:
    st.subheader("Mapa (día seleccionado)")
    df_map = df_day.dropna(subset=["lat","lon"])
    if not df_map.empty:
        st.pydeck_chart(pdk.Deck(
            initial_view_state=pdk.ViewState(
                latitude=float(df_map["lat"].mean()),
                longitude=float(df_map["lon"].mean()),
                zoom=15
            ),
            layers=[
                pdk.Layer(
                    "ScatterplotLayer",
                    data=df_map,
                    get_position='[lon, lat]',
                    get_radius=radius,
                    pickable=True,
                    get_fill_color='[255, 0, 0]',
                )
            ],
            tooltip={"text": "Drop #{drop_id}\n{dt}\nlat={lat}\nlon={lon}\nalt={alt} m\nspeed={speed_mps} m/s\nsats={sats}\nfix_ok={fix_ok}"}
        ))
elif not PYDECK_AVAILABLE:
    st.info("Pydeck no instalado → el mapa no se mostrará.")

# Tabla
st.subheader("Tabla de Datos")
if not df_day.empty:
    st.dataframe(df_day.sort_values("ts", ascending=False), width="stretch", height=350)
else:
    if not df_all.empty and day != default_day:
        st.info("No hay datos para la fecha seleccionada. Prueba con el día más reciente.")
    else:
        st.info("No hay datos para mostrar.")

# =========================================================
# Idle loop (mantener conexión viva) — solo cuando NO hay descarga
# =========================================================
if not ss.log_collecting:
    if ss.mqtt_client:
        t0 = time.time()
        # pequeño “latido” de ~0.8s que llama a loop repetidamente
        while time.time() - t0 < 0.8:
            ss.mqtt_client.loop(timeout=0.1)
    time.sleep(0.2)
    st.rerun()
