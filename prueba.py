# Streamlit + HiveMQ Cloud — Versión Estable con Persistencia en CSV y Sin Hilos
import streamlit as st
import time, ssl, os, json
from datetime import datetime, date
import pandas as pd
import paho.mqtt.client as mqtt

# El archivo donde se guardarán los datos de forma permanente
DATA_FILE = "drone_data.csv"

# Pydeck es opcional si no se instala
try:
    import pydeck as pdk
    PYDECK_AVAILABLE = True
except ImportError:
    PYDECK_AVAILABLE = False

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
T_CMD     = f"drone/{DEV_ID}/cmd"
T_STATE   = f"drone/{DEV_ID}/state"
T_INFO    = f"drone/{DEV_ID}/info"
T_PREVIEW = f"drone/{DEV_ID}/preview"
T_LOGPART = f"drone/{DEV_ID}/log/part"
T_EVENTS  = f"drone/{DEV_ID}/events"

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
    
    # --- NUEVO: Carga de datos persistentes y set para duplicados ---
    ss.seen_points = set()
    try:
        if os.path.exists(DATA_FILE):
            df = pd.read_csv(DATA_FILE)
            ss.preview_rows = df.to_dict('records')
            # Poblar el set para una rápida verificación de duplicados
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
    rc_value = rc.value if not isinstance(rc, int) else rc
    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  on_disconnect rc={rc_value}")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode("utf-8", errors="ignore")
        if topic == T_PREVIEW:
            data = json.loads(payload)
            if isinstance(data, list) and data:
                # --- NUEVO: Lógica para filtrar duplicados ---
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
                    
                    # Guardar en el archivo CSV
                    new_df = pd.DataFrame(new_rows)
                    header = not os.path.exists(DATA_FILE)
                    new_df.to_csv(DATA_FILE, mode='a', header=header, index=False)
                else:
                    ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Datos recibidos eran duplicados.")
        # ... (resto de la lógica de on_message)
    except Exception as e:
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Error en on_message: {e}")

# --- Funciones de Conexión y Publicación ---
def connect_mqtt():
    if ss.mqtt_client: return
    try:
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Creando cliente MQTT...")
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"st-web-{int(time.time())}", transport="websockets")
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
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  ERROR AL CONECTAR: {e}")
        ss.mqtt_client = None

def disconnect_mqtt():
    if ss.mqtt_client:
        ss.mqtt_client.disconnect()
        ss.mqtt_client = None
        ss.mqtt_connected = False
        ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Cliente desconectado.")

def mqtt_publish(topic, payload_obj):
    if ss.mqtt_client and ss.mqtt_connected:
        try:
            ss.mqtt_client.publish(topic, json.dumps(payload_obj), qos=1)
            ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  CMD -> {payload_obj}")
            return True
        except Exception as e:
            ss.diag.append(f"{datetime.now().strftime('%H:%M:%S')}  Error al publicar: {e}")
            return False
    else:
        st.warning("No se puede publicar, cliente no conectado.")
        return False

# --- Barra Lateral (UI) ---
login_box()
with st.sidebar:
    st.markdown("---")
    st.subheader("Conexión")
    if not ss.mqtt_client:
        if st.button("🔌 Conectar a MQTT", use_container_width=True):
            connect_mqtt()
            st.rerun()
    else:
        if st.button("🔌 Desconectar", use_container_width=True, type="primary"):
            disconnect_mqtt()
            st.rerun()

    st.subheader("Estado")
    if ss.mqtt_connected: st.success("🟢 Conectado")
    else: st.error("🔴 Desconectado")
    
    st.subheader("Opciones")
    ss.insecure_tls = st.checkbox("Usar TLS Inseguro (Debug)", value=ss.insecure_tls)

# --- Bucle Principal / "Tick" ---
if ss.mqtt_client:
    ss.mqtt_client.loop(timeout=0.1)

# -------- Controles Principales --------
st.markdown("---")
left, mid, right = st.columns([1.4,1,1])
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
                    st.success("Comando de INICIO enviado con nuevos parámetros.")
            else: st.warning("Necesitas ingresar el PIN para iniciar la misión.")

with mid:
    st.subheader("Paro de emergencia")
    if st.button("⏹️ Paro Inmediato", type="primary", use_container_width=True):
        if mqtt_publish(T_CMD, {"action":"stop"}):
            st.info("Comando STOP enviado")

with right:
    st.subheader("Solicitar Datos")
    pvN = st.number_input("Puntos de preview:", 1, 2000, 50, 50)
    if st.button("📥 Solicitar Preview", use_container_width=True):
        if mqtt_publish(T_CMD, {"action":"preview", "last": int(pvN)}):
            st.info("Solicitud de preview enviada.")
    if st.button("⬇️ Descargar Log Completo", use_container_width=True):
        ss.log_chunks = []; ss.log_seq_last = -1; ss.log_eof = False
        if mqtt_publish(T_CMD, {"action":"stream_log"}):
            st.info("Solicitud de log completo enviada.")

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


# -------- CSV download (from MQTT chunks) --------
st.markdown("---")
st.subheader("Descarga de Log Completo")
if ss.log_chunks:
    st.caption(f"Log chunks received: {len(ss.log_chunks)} | EOF: {ss.log_eof}")
    csv_bytes = "".join(ss.log_chunks).encode("utf-8", errors="ignore")
    st.download_button(
        "💾 Descargar CSV",
        data=csv_bytes,
        file_name=f"drops-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True
    )
else:
    st.caption("Haz clic en **Descargar Log Completo** para activar la descarga.")

# -------- Logs de Diagnóstico --------
with st.expander("🔍 Ver Logs de Diagnóstico"):
    st.code("\n".join(ss.diag[-100:]), language="log")

# --- Auto-refresco ---
time.sleep(8)
st.rerun()



