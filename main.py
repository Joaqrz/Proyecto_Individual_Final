#Libraries section:

from typing import Union

from fastapi import FastAPI

import pandas as pd
 
import ast 

import json
 
import gzip


#API and functions section:
app = FastAPI()


@app.get("/docs")

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

@app.get("/playtimegenre/{genre}") #PlayTime per genre function 
def playtimegenre():
    return {"El año más jugado para el género " "fue el año:" }

@app.get("/UserForGenre/{genre}") # User with most hours played per genre function 
def userforgenre():
    return {"El usuario que acumula más horas para el género "" es " "y jugó en:"}

@app.get("/UserRecommend/{year}") # Most played gamer per year function 
def userrecomnend():
    return {"El top 3 de juegos más jugados para el año " "es:"}

@app.get("/SentimentAnalysis/{developer}") # Sentiment analysis per developer function 
def sentimentanalysis():
    return {"devolver diccionario"} 

#ETL section:

#ETL for games:
data = []

with gzip.open('steam_games.json.gz', 'rb') as d:
    for line in d:
        data.append(json.loads(line))
games = pd.DataFrame(data)
d.close()

#ETL for user reviews:
data = []

try:
    with gzip.open('user_reviews.json.gz', 'rb') as d:
        for line in d:
            data.append(ast.literal_eval(line.decode('utf-8')))
    reviews = pd.DataFrame(data)
finally:
    d.close()  # Cerramos el archivo en el bloque 'finally' para garantizar que se cierre correctamente

#ETL for user items:
data = []

try:
    with gzip.open('users_items.json.gz', 'rb') as d:
        for line in d:
            data.append(ast.literal_eval(line.decode('utf-8')))
    Users_Items = pd.DataFrame(data)
finally:
    d.close()
    
#NaN cleaning:

games_clean = games.dropna(how='all')
reviews_clean = reviews.dropna(how='all')
items_clean = Users_Items.dropna(how='all')