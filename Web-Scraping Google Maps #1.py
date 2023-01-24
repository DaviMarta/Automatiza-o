#importar pacotes necessários
import pandas as pd
import geopandas as gpd
import requests
import json
import shapely
import fiona
from pyproj import Proj, transform, Transformer
from sqlalchemy import create_engine
import psycopg2
import geoalchemy2
import os
keywords = ["loja de roupa","mecanica","sapato","suplemento","odontologia", "dentista","mercado","ortopetista","farmacia","acabamento","sacola"]

for keyword in keywords:


    def csv_to_point(non_spatial_data):

        #crie o geodataframe e exporte-o como um arquivo de ponto
        del non_spatial_data['Tags']
        spatial_df = gpd.GeoDataFrame(non_spatial_data, geometry=gpd.points_from_xy(non_spatial_data.Longitude, non_spatial_data.Latitude))
        #spatial_df.to_csv("point_data.csv")
        print(spatial_df)
        #spatial_df.to_file("point_data.shp")

        #create a projection file that corresponds to where data was taken from
        #prj = open("point_data.prj", "w")
        epsg = 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'
        #prj.write(epsg)
        #prj.close()

        return spatial_df

    def find_locations(search_url, api_key):


        #lista de listas para todos os dados
        final_data = []

        #while loop para solicitar e analisar os arquivos JSON solicitados
        while True:
            jj = requests.get(search_url).json()
            results = jj['results']
            #analise todas as informações necessárias
            for result in results:
                name = result['name']
                place_id = result['place_id']
                lat = result['geometry']['location']['lat']
                longi = result['geometry']['location']['lng']
                rating = result['rating']
                types = result['types']
                data = [name, place_id, lat, longi, rating, types]
                final_data.append(data)
            
             #se houver uma próxima página, o loop será reiniciado com uma url atualizada
             #se não houver próxima página, o programa grava em um csv e salva em df    
            if 'next_page_token' not in jj.keys():
                labels = ['Place Name','ID_Field', 'Latitude', 'Longitude', 'Rating', 'Tags']
                location_df = pd.DataFrame.from_records(final_data, columns=labels)
                #location_df.to_csv('location.csv')
                break
            else:
                next_page_token = jj['next_page_token']
                search_url = f'https://maps.googleapis.com/maps/api/place/nearbysearch/json?key={api_key}&pagetoken={next_page_token}'

        return (final_data, location_df)

    def find_details(final_data, api_key):

        final_detailed_data =[]

        #Usa o ID exclusivo de cada local para usar outra solicitação de API para obter informações de telefone e site de cada empresa.
        for places in final_data:
            id_field = places[1]
            req_url = f'https://maps.googleapis.com/maps/api/place/details/json?place_id={id_field}&fields=name,formatted_phone_number,website&key={api_key}'
            jj = requests.get(req_url).json()
            # print(jj)
            results = jj['result']
            identification = id_field
            
            if results.get("formatted_phone_number") is None:
                continue
            phone = results["formatted_phone_number"]
            if result.get("website") is None:
                continue
            website = results["website"]
            title = results["name"]
            detailed_data = [title, identification, phone, website]
            final_detailed_data.append(detailed_data)

        columns = ["Business", "ID_Field","Phone", "Website"]
        details_df = pd.DataFrame.from_records(final_detailed_data, columns=columns)
        details_df.to_csv('further_details.csv')
        
        return details_df

    def join_data(details_df,location_df):

        final_sheet = location_df.join(details_df.set_index('ID_Field'), on='ID_Field')

        final_sheet.to_csv(str(keyword) + ".csv")

        print(final_sheet)

        return final_sheet

        
    def main():
        # TODO: Read a json file for load configs.
        #assigning Parâmetros para pesquisa de localização
        api_key = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
        search_radius = '100000'
        
        coords = '-X.XXXXXXXXXXXXXXXXXX, -X.XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
        request_url = f'https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={coords}&radius={search_radius}&keyword={keyword}&key={api_key}'

        #encontre os locais dos estabelecimentos desejados no google maps
        final_data, location_df = find_locations(request_url, api_key)

        #encontre site, telefone e avaliações de estabelecimentos
        details_df = find_details(final_data, api_key)

        #junte os dois dataframes para ter um produto final
        non_spatial_data = join_data(details_df,location_df)

        #c
        spatial_df = csv_to_point(non_spatial_data)



    if __name__ == "__main__":
        main()

path = r ""#COLOCAR O LOCAL ONDE ESTÁ OS EXCELS SALVOS

# use compreensão de lista para criar uma lista de arquivos csv
csv_files = [file for file in os.listdir(path) if file.endswith('.csv')]

# cria uma lista vazia para armazenar os DataFrames
df_list = []

# iterar pelos arquivos csv
for csv_file in csv_files:
    # lê o arquivo csv atual em um DataFrame
    df = pd.read_csv(os.path.join(path, csv_file))
    # adiciona uma nova coluna 'file_name' com o nome do arquivo csv atual
    df['name'] = os.path.basename(csv_file)
    # anexa o DataFrame atual à lista
    df_list.append(df)

# concatenar todos os DataFrames em um único DataFrame
merged_df = pd.concat(df_list)

# grava o DataFrame mesclado em um novo arquivo csv
merged_df.to_excel("all.xlsx", index=False)
