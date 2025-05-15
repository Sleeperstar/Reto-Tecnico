import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from PIL import Image # Import Pillow for image handling
import os # Importar os para leer variables de entorno

# --- PARÁMETROS DE CONEXIÓN A POSTGRESQL ---
# Ahora se leen desde variables de entorno.
# Define valores por defecto si las variables de entorno no están configuradas
# (útil para desarrollo local, pero asegúrate de configurar las variables en producción).
DB_NAME = os.environ.get('DB_NAME', 'tu_basededatos')
DB_USER = os.environ.get('DB_USER', 'tu_usuario')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'tu_contraseña')
DB_HOST = os.environ.get('DB_HOST', 'tu_host')
DB_PORT = os.environ.get('DB_PORT', '5432')

CONN_PARAMS = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'port': DB_PORT
}

# --- Cache para la conexión a la base de datos ---
@st.cache_resource # Usar cache_resource para conexiones
def init_connection():
    """
    Inicializa la conexión a la base de datos PostgreSQL.
    Retorna el objeto de conexión.
    """
    # Verificar si las variables de entorno esenciales están configuradas
    # (especialmente en un entorno de producción)
    if CONN_PARAMS['dbname'] == 'tu_basededatos' or \
       CONN_PARAMS['user'] == 'tu_usuario' or \
       CONN_PARAMS['password'] == 'tu_contraseña' or \
       CONN_PARAMS['host'] == 'tu_host':
        st.warning(
            "Advertencia: Usando parámetros de conexión por defecto. "
            "Asegúrate de configurar las variables de entorno DB_NAME, DB_USER, DB_PASSWORD, DB_HOST y DB_PORT "
            "para tu entorno de despliegue (ej. en Streamlit Cloud Secrets)."
        )

    try:
        conn = psycopg2.connect(**CONN_PARAMS)
        return conn
    except psycopg2.OperationalError as e:
        st.error(f"Error al conectar con PostgreSQL: {e}")
        st.error(
            "Por favor, verifica los parámetros de conexión. Si estás desplegando, "
            "asegúrate de que las variables de entorno (Secrets en Streamlit Cloud) "
            "estén configuradas correctamente y que la base de datos sea accesible."
        )
        st.stop() # Detiene la ejecución del script si la conexión falla
    except Exception as e:
        st.error(f"Un error inesperado ocurrió durante la conexión: {e}")
        st.stop()

# --- Cache para la carga de datos ---
@st.cache_data(ttl=600) # Cachea los datos por 10 minutos
def run_query(_conn, query, params=None):
    """
    Ejecuta una consulta SQL y retorna los resultados como un DataFrame de Pandas.
    Args:
        _conn: Objeto de conexión a la base de datos.
        query (str): Consulta SQL a ejecutar.
        params (tuple, optional): Parámetros para la consulta SQL.
    """
    try:
        return pd.read_sql_query(query, _conn, params=params)
    except Exception as e:
        st.error(f"Error al ejecutar la consulta: {query}")
        st.error(f"Detalles del error: {e}")
        return pd.DataFrame() # Retorna un DataFrame vacío en caso de error

# --- Configuración de la página de Streamlit ---
st.set_page_config(page_title="Netflix Data Explorer", layout="wide")
st.title("🎬 Explorador de Datos de Netflix")

# --- Inicializar conexión ---
conn = init_connection()

# --- Barra lateral para navegación y filtros ---
st.sidebar.header("Navegación")

# --- Añadir el logo a la barra lateral ---
try:
    # Intenta cargar la imagen. Asegúrate que 'NetflixLogo.png' esté en el mismo directorio.
    logo = Image.open("NetflixLogo.png")
    st.sidebar.image(logo, use_container_width=True)
except FileNotFoundError:
    st.sidebar.warning("Logo 'NetflixLogo.png' no encontrado. Colócalo en el directorio del script.")
except Exception as e:
    st.sidebar.error(f"Error al cargar el logo: {e}")


option = st.sidebar.selectbox(
    "Selecciona una vista:",
    ("Inicio", "Ver Tablas Completas", "Análisis de Shows", "Buscar Shows por Título")
)

# --- Lógica de las diferentes vistas ---

if option == "Inicio":
    st.header("Bienvenido al Explorador de Datos de Netflix")
    st.markdown("""
    Utiliza la barra lateral para navegar por las diferentes secciones:
    - **Ver Tablas Completas**: Visualiza el contenido de cada una de las tablas normalizadas.
    - **Análisis de Shows**: Explora estadísticas y gráficos sobre los títulos disponibles.
    - **Buscar Shows por Título**: Realiza búsquedas específicas en la tabla de shows.

    Esta aplicación se conecta a una base de datos PostgreSQL que contiene datos procesados
    de Netflix.
    """)
    st.info("Asegúrate de que tu base de datos PostgreSQL esté en ejecución y accesible.")

    st.markdown("---") # Separador visual
    st.subheader("Consulta Rápida: Shows Añadidos por Año")

    min_year_query = "SELECT MIN(year_added) FROM shows WHERE year_added IS NOT NULL;"
    max_year_query = "SELECT MAX(year_added) FROM shows WHERE year_added IS NOT NULL;"
    
    df_min_year = run_query(conn, min_year_query)
    df_max_year = run_query(conn, max_year_query)

    current_year = pd.Timestamp.now().year
    min_val = int(df_min_year.iloc[0,0]) if not df_min_year.empty and pd.notna(df_min_year.iloc[0,0]) else 1900
    max_val = int(df_max_year.iloc[0,0]) if not df_max_year.empty and pd.notna(df_max_year.iloc[0,0]) else current_year

    year_input = st.number_input(
        "Ingresa un año para ver cuántos títulos fueron añadidos:",
        min_value=min_val,
        max_value=max_val,
        value=max_val, 
        step=1,
        format="%d"
    )

    if st.button("Consultar Shows Añadidos en el Año"):
        if year_input:
            query_year_added = """
                SELECT type, COUNT(*) as count
                FROM shows
                WHERE year_added = %s
                GROUP BY type;
            """
            df_year_results = run_query(conn, query_year_added, params=(int(year_input),))

            if not df_year_results.empty:
                message = f"En el año {year_input} se añadieron:\n"
                for _, row in df_year_results.iterrows():
                    tipo = "Películas" if row['type'] == 'Movie' else "Series de TV" if row['type'] == 'TV Show' else row['type']
                    message += f"- {row['count']} {tipo}\n"
                st.success(message)
            else:
                st.info(f"No se encontraron títulos añadidos en el año {year_input}.")
        else:
            st.warning("Por favor, ingresa un año válido.")


elif option == "Ver Tablas Completas":
    st.header("Visualización de Tablas Completas")
    
    table_names = {
        "Directores": "directors",
        "Miembros del Elenco": "cast_members",
        "Países": "countries",
        "Géneros": "genres",
        "Shows (Títulos)": "shows",
        "Relación Show-Elenco": "show_cast_members",
        "Relación Show-Países": "show_countries",
        "Relación Show-Géneros": "show_genres"
    }
    
    selected_table_display_name = st.selectbox("Selecciona una tabla para mostrar:", list(table_names.keys()))
    
    if selected_table_display_name:
        db_table_name = table_names[selected_table_display_name]
        st.subheader(f"Contenido de la tabla: {db_table_name}")
        
        query = f"SELECT * FROM {db_table_name};"
        df_table = run_query(conn, query)
        
        if not df_table.empty:
            st.dataframe(df_table, use_container_width=True)
            st.write(f"Total de filas: {len(df_table)}")
        else:
            st.warning(f"No se encontraron datos o la tabla '{db_table_name}' está vacía.")

elif option == "Análisis de Shows":
    st.header("Análisis Detallado de Shows")
    
    # Cargar datos necesarios para los análisis
    query_shows = "SELECT show_id, type, title, director_id, release_year, rating, duration, month_added FROM shows;"
    df_shows = run_query(conn, query_shows)

    query_directors = "SELECT director_id, director_name FROM directors;"
    df_directors = run_query(conn, query_directors)

    query_show_cast = "SELECT show_id, cast_member_id FROM show_cast_members;"
    df_show_cast = run_query(conn, query_show_cast)
    query_cast_members = "SELECT cast_member_id, cast_member_name FROM cast_members;"
    df_cast_members = run_query(conn, query_cast_members)

    query_show_countries = "SELECT show_id, country_id FROM show_countries;"
    df_show_countries = run_query(conn, query_show_countries)
    query_countries = "SELECT country_id, country_name FROM countries;"
    df_countries = run_query(conn, query_countries)

    query_show_genres = "SELECT show_id, genre_id FROM show_genres;"
    df_show_genres = run_query(conn, query_show_genres)
    query_genres = "SELECT genre_id, genre_name FROM genres;"
    df_genres = run_query(conn, query_genres)


    if not df_shows.empty:
        # --- 1. Conteo de Tipos (Movie vs TV Show) ---
        st.subheader("Distribución por Tipo (Película / Serie de TV)")
        type_counts = df_shows['type'].value_counts().reset_index()
        type_counts.columns = ['type', 'count']
        fig_type = px.pie(type_counts, names='type', values='count', title="Películas vs. Series de TV",
                          color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig_type, use_container_width=True)

        # --- 2. Shows por Año de Lanzamiento ---
        st.subheader("Número de Shows por Año de Lanzamiento")
        df_shows_release = df_shows.dropna(subset=['release_year'])
        if not df_shows_release.empty:
            df_shows_release['release_year'] = df_shows_release['release_year'].astype(int)
            release_year_counts = df_shows_release['release_year'].value_counts().sort_index().reset_index()
            release_year_counts.columns = ['release_year', 'count']
            latest_years_count = release_year_counts.tail(30) 

            fig_release_year = px.bar(latest_years_count, x='release_year', y='count', 
                                      title="Shows por Año de Lanzamiento (Últimos años)",
                                      color_discrete_sequence=px.colors.sequential.Viridis)
            fig_release_year.update_xaxes(type='category') 
            st.plotly_chart(fig_release_year, use_container_width=True)
        else:
            st.warning("No hay datos de 'release_year' para mostrar el gráfico.")

        # --- 3. Top Ratings ---
        st.subheader("Distribución de Ratings")
        if 'rating' in df_shows.columns:
            df_ratings = df_shows.dropna(subset=['rating'])
            if not df_ratings.empty:
                rating_counts = df_ratings['rating'].value_counts().nlargest(10).reset_index() 
                rating_counts.columns = ['rating', 'count']
                fig_rating = px.bar(rating_counts, x='rating', y='count', title="Top 10 Ratings más comunes",
                                    color_discrete_sequence=px.colors.qualitative.Bold)
                st.plotly_chart(fig_rating, use_container_width=True)
            else:
                st.warning("No hay datos de 'rating' para mostrar el gráfico.")
        else:
            st.warning("La columna 'rating' no se encontró en la tabla 'shows'.")

        # --- 4. Shows por mes en que fueron añadidos ---
        st.subheader("Shows añadidos por Mes")
        if 'month_added' in df_shows.columns:
            month_map = {1: 'Ene', 2: 'Feb', 3: 'Mar', 4: 'Abr', 5: 'May', 6: 'Jun', 
                         7: 'Jul', 8: 'Ago', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dic'}
            df_shows_month = df_shows.dropna(subset=['month_added'])
            if not df_shows_month.empty:
                df_shows_month['month_added_name'] = df_shows_month['month_added'].astype(int).map(month_map)
                month_added_counts = df_shows_month['month_added_name'].value_counts().reset_index()
                month_added_counts.columns = ['month_added_name', 'count']
                all_months_df = pd.DataFrame({'month_added_name': list(month_map.values())})
                month_added_counts = pd.merge(all_months_df, month_added_counts, on='month_added_name', how='left').fillna(0)
                month_order_map = {name: i for i, name in enumerate(month_map.values())}
                month_added_counts['month_order'] = month_added_counts['month_added_name'].map(month_order_map)
                month_added_counts = month_added_counts.sort_values('month_order')

                fig_month_added = px.bar(month_added_counts, x='month_added_name', y='count', 
                                         title="Número de Shows añadidos por Mes",
                                         color_discrete_sequence=px.colors.qualitative.Set2)
                st.plotly_chart(fig_month_added, use_container_width=True)
            else:
                st.warning("No hay datos de 'month_added' para mostrar el gráfico.")
        else:
            st.warning("La columna 'month_added' no se encontró en la tabla 'shows'.")
        
        st.markdown("---")
        # --- 5. Top 10 Directores ---
        st.subheader("Top 10 Directores con Más Shows")
        if not df_directors.empty and not df_shows.empty:
            df_shows_with_directors = pd.merge(df_shows, df_directors, on='director_id', how='inner')
            # Filtrar directores 'Unknown' antes de contar
            df_filtered_directors = df_shows_with_directors[df_shows_with_directors['director_name'].str.lower() != 'unknown']
            
            if not df_filtered_directors.empty:
                director_counts = df_filtered_directors['director_name'].value_counts().nlargest(10).reset_index()
                director_counts.columns = ['director_name', 'count']
                fig_directors = px.bar(director_counts, x='director_name', y='count', 
                                       title="Top 10 Directores (excluyendo 'Unknown')", 
                                       color='count', # Usar 'count' para el degradado
                                       color_continuous_scale=px.colors.sequential.Plasma,
                                       labels={'count':'Número de Shows'})
                st.plotly_chart(fig_directors, use_container_width=True)
            else:
                st.warning("No hay datos de directores (después de filtrar 'Unknown') para mostrar.")
        else:
            st.warning("No se pudieron cargar datos de directores o shows.")

        # --- 6. Top 10 Miembros del Elenco ---
        st.subheader("Top 10 Miembros del Elenco con Más Apariciones")
        if not df_show_cast.empty and not df_cast_members.empty:
            df_full_cast_info = pd.merge(df_show_cast, df_cast_members, on='cast_member_id', how='inner')
            cast_counts = df_full_cast_info['cast_member_name'].value_counts().nlargest(10).reset_index()
            cast_counts.columns = ['cast_member_name', 'count']
            fig_cast = px.bar(cast_counts, x='cast_member_name', y='count', 
                              title="Top 10 Miembros del Elenco", 
                              color='count', # Usar 'count' para el degradado
                              color_continuous_scale=px.colors.sequential.Cividis,
                              labels={'count':'Número de Apariciones'})
            st.plotly_chart(fig_cast, use_container_width=True)
        else:
            st.warning("No se pudieron cargar datos del elenco.")

        # --- 7. Top 10 Países Productores ---
        st.subheader("Top 10 Países Productores de Contenido")
        if not df_show_countries.empty and not df_countries.empty:
            df_full_country_info = pd.merge(df_show_countries, df_countries, on='country_id', how='inner')
            country_counts = df_full_country_info['country_name'].value_counts().nlargest(10).reset_index()
            country_counts.columns = ['country_name', 'count']
            fig_countries = px.bar(country_counts, x='country_name', y='count',
                                   title="Top 10 Países Productores", 
                                   color='count', # Usar 'count' para el degradado
                                   color_continuous_scale=px.colors.sequential.Blues,
                                   labels={'count':'Número de Shows'})
            st.plotly_chart(fig_countries, use_container_width=True)
        else:
            st.warning("No se pudieron cargar datos de países.")

        # --- 8. Top 10 Géneros ---
        st.subheader("Top 10 Géneros Más Comunes")
        if not df_show_genres.empty and not df_genres.empty:
            df_full_genre_info = pd.merge(df_show_genres, df_genres, on='genre_id', how='inner')
            genre_counts = df_full_genre_info['genre_name'].value_counts().nlargest(10).reset_index()
            genre_counts.columns = ['genre_name', 'count']
            fig_genres = px.bar(genre_counts, x='genre_name', y='count',
                                title="Top 10 Géneros", 
                                color='count', # Usar 'count' para el degradado
                                color_continuous_scale=px.colors.sequential.Greens,
                                labels={'count':'Número de Shows'})
            st.plotly_chart(fig_genres, use_container_width=True)
        else:
            st.warning("No se pudieron cargar datos de géneros.")
            
        st.markdown("---")
        # Los gráficos de duración han sido eliminados según la solicitud.

    else:
        st.warning("No se pudieron cargar datos de la tabla 'shows' para el análisis.")

elif option == "Buscar Shows por Título":
    st.header("Buscar Shows por Título")
    search_term = st.text_input("Ingresa parte del título a buscar:", "")

    if search_term:
        query_search = "SELECT show_id, type, title, release_year, rating, description FROM shows WHERE title ILIKE %s;"
        try:
            df_search_results = run_query(conn, query_search, params=(f"%{search_term}%",))

            if not df_search_results.empty:
                st.subheader(f"Resultados para '{search_term}':")
                st.dataframe(df_search_results, use_container_width=True)
            else:
                st.info(f"No se encontraron shows que coincidan con '{search_term}'.")
        except Exception as e: 
            st.error(f"Error durante la búsqueda: {e}")


# --- Pie de página (opcional) ---
st.sidebar.markdown("---")
st.sidebar.info("Aplicación creada con Streamlit y PostgreSQL.")
