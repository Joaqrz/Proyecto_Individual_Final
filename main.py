#Libraries section:

from typing import Union

from fastapi import FastAPI

from fastapi.openapi.docs import get_swagger_ui_html

import pandas as pd

from sklearn.metrics.pairwise import cosine_similarity
 


#API and functions section:
app = FastAPI()


@app.get("/")
async def read_root():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Docs")

#------------------------------------------------------------------------------------------------------------#

@app.get("/PlayTimeGenre/{genero}") #PlayTime por genero 
async def PlayTimeGenre(genero: str):
    try:
        # Leemos el archivo parquet
        df_genero = pd.read_parquet("src/endpoint1.parquet")
        
        # Filtrar el DataFrame por el género especificado
        df_genero = df_genero[df_genero["genres"] == genero]
        
        # Encontrar el año con más horas jugadas para el género
        año_con_mas_horas = list(df_genero[df_genero["playtime"] == df_genero["playtime"].max()]["release"])[0]
        
        return {f"Año de lanzamiento con más horas jugadas para {genero}": año_con_mas_horas}
    
    except Exception as e:
        return {"error": str(e)}

#------------------------------------------------------------------------------------------------------------#

@app.get("/UserForGenre/{genre}") #usuario con más horas jugadas por género 
async def UserForGenre(genero: str):
    try:
        #Leemos el parquet
        endpoint2 = pd.read_parquet('src/endpoint2.parquet')
        
        #Convertimos los minutos a horas
        endpoint2['playtime'] = round(endpoint2['playtime']/60,2)
        
        #Filtramos el género solicitado
        endpoint2_genero = endpoint2[endpoint2['genres'] == genero]
        
        #Buscamos el usuario con más horas en el género
        usuario_con_mas_horas = endpoint2_genero.loc[endpoint2_genero['playtime'].idxmax()]['user_id']
        
        #Agrupamos por año 
        horas_por_año_usuario = endpoint2_genero[endpoint2_genero['user_id'] == usuario_con_mas_horas]
        horas_por_año_usuario = horas_por_año_usuario.groupby('release')['playtime'].sum().reset_index()
        horas_por_año_usuario = horas_por_año_usuario.rename(columns = {'release': 'Año','playtime':'Horas'})

        #Creamos la listita de acumulacion de horas jugadas por año
        lista_horas_por_año = horas_por_año_usuario.to_dict(orient='records')
        return {f"Usuario con más horas jugadas para {genero}": usuario_con_mas_horas,
            "Horas jugadas": lista_horas_por_año}
    except Exception as e:
        return {"error": str(e)}
    
#------------------------------------------------------------------------------------------------------------#

@app.get("/UserRecommend/{year}") # Juego más recomendado por los usuarios anualmente 
async def UsersRecommend(year: int):
    try:
        # Leemos el archivo
        df = pd.read_parquet('src/reviews.parquet')
        #print("DataFrame cargado:", df.head())  # Verificamos los primeros registros del DataFrame
        
        # Filtramos por año
        df_year = df[df['posted_year'] == year]
        #print("Registros para el año", year, ":", len(df_year))  # Verificamos cuántos registros hay para el año específico
        
        # Filtramos por recomendaciones positivas/neutrales
        df_recommend = df_year[df_year['recommend'] == True]
        df_sentiment = df_recommend[df_recommend['sentiment_analysis'].isin([2, 1])]
        #print("Registros con recomendaciones positivas/neutrales:", len(df_sentiment))  # Verificamos cuántos registros cumplen con los criterios de recomendación y sentimiento
        
        # Excluimos títulos 'Otros'
        df_filtered = df_sentiment[df_sentiment['title'] != 'Otros']
        # print("Registros después de excluir 'Otros':", len(df_filtered))  # Verificamos cuántos registros quedan después de excluir 'Otros'
        
        # Agrupamos por título y contamos las recomendaciones
        recommendations = df_filtered.groupby('title')['recommend'].sum()
        #print("Recomendaciones por juego:", recommendations)  # Verificamos el conteo de recomendaciones por juego
        
        # Ordenamos las recomendaciones por número de recomendaciones 
        recommendations_sorted = recommendations.sort_values(ascending=False)
        
        # Tomamos los tres primeros juegos
        top_3 = recommendations_sorted.head(3)
        #print("Top 3 juegos recomendados:", top_3)  # Verificamos el top 3 de juegos recomendados
        
        # Verificamos si hay suficientes juegos recomendados
        if len(top_3) >= 3:
            # Creamos una lista de diccionarios para los tres primeros juegos
            result = [{"Puesto {}".format(i + 1): game} for i, game in enumerate(top_3.index)]
        else:
            # Si no hay suficientes juegos, devolvemos un mensaje de datos insuficientes
            result = 'Datos insuficientes'
        
        return result
    
    except Exception as e:
        # Capturamos cualquier excepción y la devolvemos como un diccionario
        return {"error": str(e)}
    
#------------------------------------------------------------------------------------------------------------#

@app.get('/UsersWorstDeveloper/{year}')
def UsersWorstDeveloper(year:int):
    try:
        # Leemos el archivo
        df = pd.read_parquet('src/reviews.parquet')
        #print("DataFrame cargado:", df.head())  # Verificamos los primeros registros del DataFrame
        
        # Filtramos por año
        df_year = df[df['posted_year'] == year]
        #print("Registros para el año", year, ":", len(df_year))  # Verificamos cuántos registros hay para el año específico
        
        # Filtramos por recomendaciones positivas/neutrales
        df_recommend = df_year[df_year['recommend'] == False]
        df_sentiment = df_recommend[df_recommend['sentiment_analysis'] == 0]
        #print("Registros con recomendaciones positivas/neutrales:", len(df_sentiment))  # Verificamos cuántos registros cumplen con los criterios de recomendación y sentimiento
        
        # Excluimos títulos 'Otros'
        df_filtered = df_sentiment[df_sentiment['developer'] != 'Otros']
        # print("Registros después de excluir 'Otros':", len(df_filtered))  # Verificamos cuántos registros quedan después de excluir 'Otros'
        
        # Agrupamos por título y contamos las recomendaciones
        negative_recommendations = df_filtered.groupby('developer')['recommend'].sum()
        #print("Recomendaciones por juego:", recommendations)  # Verificamos el conteo de recomendaciones por juego
        
        # Ordenamos las recomendaciones por número de recomendaciones 
        negative_recommendations_sorted = negative_recommendations.sort_values(ascending=False)
        
        # Tomamos los tres primeros juegos
        top_3 = negative_recommendations_sorted.head(3)
        #print("Top 3 juegos recomendados:", top_3)  # Verificamos el top 3 de juegos recomendados
        
        # Verificamos si hay suficientes juegos recomendados
        if len(top_3) >= 3:
            # Creamos una lista de diccionarios para los tres primeros juegos
            result = [{"Puesto {}".format(i + 1): game} for i, game in enumerate(top_3.index)]
        else:
            # Si no hay suficientes juegos, devolvemos un mensaje de datos insuficientes
            result = 'Datos insuficientes'
        
        return result
    
    except Exception as e:
        # Capturamos cualquier excepción y la devolvemos como un diccionario
        return {"error": str(e)}
    
#------------------------------------------------------------------------------------------------------------#

@app.get("/SentimentAnalysis/{developer}") # Sentiment analysis per developer function 
def SentimentAnalysis(desarrolladora:str):
    try:
        #Leemos el archivo    
        df_sentiment = pd.read_parquet('src/reviews.parquet')
        
        #Filtramos el df con la desarrolladora
        df_developer = df_sentiment[df_sentiment['developer'] == desarrolladora]
        
        #Contabilizamos las reviews
        positive_count = df_developer[df_developer['sentiment_analysis'] == 2].shape[0]
        neutral_count = df_developer[df_developer['sentiment_analysis'] == 1].shape[0]
        negative_count = df_developer[df_developer['sentiment_analysis'] == 0].shape[0]
        
        #Creamos el diccionario solicitado:
        result_dicc = {
            'Desarrolladora': desarrolladora,
            'Reviews Positivas': positive_count,
            'Reviews Neutras': neutral_count,
            'Reviews Negativas': negative_count
        }
        return result_dicc
        
    except Exception as e:
        # Capturamos cualquier excepción y la devolvemos como un diccionario
        return {"error": str(e)}

#------------------------------------------------------------------------------------------------------------#

@app.get("/recomendacion_juego/{id_producto}")
def recommend_games(id: int):
    '''
    Esta función recomienda 5 juegos a partir del juego ingresado.

    Args:
        game_id (int): ID único del videojuego al cual se le harán las recomendaciones.
    '''
    # Lee el dataset:
    modelo_render = pd.read_parquet('src/modelo_render.parquet')
    
    # Verifica si el juego con game_id existe en df_games
    game = modelo_render[modelo_render['id'] == id]

    if game.empty:
        return("El juego '{id}' no posee registros.")
    
    # Obtiene el índice del juego dado
    idx = game.index[0]

    # Toma una muestra aleatoria del DataFrame df_games
    sample_size = 2000  # Define el tamaño de la muestra (ajusta según sea necesario)
    df_sample = modelo_render.sample(n=sample_size, random_state=42)  # Ajusta la semilla aleatoria según sea necesario

    # Calcula la similitud de contenido solo para el juego dado y la muestra
    sim_scores = cosine_similarity([modelo_render.iloc[idx, 3:]], df_sample.iloc[:, 3:])

    # Obtiene las puntuaciones de similitud del juego dado con otros juegos
    sim_scores = sim_scores[0]

    # Ordena los juegos por similitud en orden descendente
    similar_games = [(i, sim_scores[i]) for i in range(len(sim_scores)) if i != idx]
    similar_games = sorted(similar_games, key=lambda x: x[1], reverse=True)

    # Obtiene los 5 juegos más similares
    similar_game_indices = [i[0] for i in similar_games[:5]]

    # Lista de juegos similares (solo nombres)
    similar_game_names = df_sample['title'].iloc[similar_game_indices].tolist()

    return {"similar_games": similar_game_names}
#------------------------------------------------------------------------------------------------------------#

