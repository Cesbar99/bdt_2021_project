import folium
import os
import json

# Create map object
m = folium.Map(location=[46.486835, 11.335177], zoom_start=12) # COORDINATE BOZEN 

# Global tooltip
tooltip = 'Click For More Info'

# Create custom marker icon
# logoIcon = folium.features.CustomIcon('logo.png', icon_size=(50, 50))

# Vega data
#vis = os.path.join('data', 'vis.json')

# Percorso talvera 
path_t = [(46.495619, 11.347877), (46.499680, 11.346658), (46.506887, 11.349619), (46.510753, 11.350403), (46.511931, 11.350571), (46.513579, 11.353640), (46.514927,11.357624),
(46.517354, 11.357581)]
path_pa = [(46.487316, 11.290921),(46.482711, 11.304865),(46.479067, 11.307707), (46.474282, 11.309064), (
46.467408, 11.303634), (46.458519, 11.301413), (46.445858, 11.309212)]
path_is = [(46.494716, 11.387254), (46.492876, 11.370051), (46.493996, 11.353545),(46.486874, 11.334018),(46.476998, 11.315410),(46.465194, 11.304854),
(46.450954, 11.306785)]
# Geojson Data
overlay = os.path.join('data', 'overlay.json')

# Create markers
folium.Marker([46.494716, 11.387254],
              popup='<strong>Isarco</strong>',
              tooltip=tooltip).add_to(m),
folium.Marker([46.517354, 11.357581],
              popup='<strong>Talvera</strong>',
              tooltip=tooltip).add_to(m),
folium.Marker([46.487316, 11.290921],
              popup='<strong> Ponte Adige </strong>',
              tooltip=tooltip).add_to(m),
folium.PolyLine(path_t,
                color='steelblue',
                weight= 5,
                opacity=0.8).add_to(m),
folium.PolyLine(path_pa,
                color='dodgerblue',
                weight= 5,
                opacity=0.8).add_to(m),
folium.PolyLine(path_is,
                color='cadetblue',
                weight= 5,
                opacity=0.8).add_to(m),
folium.Marker([46.494716, 11.387254],
              popup=folium.Popup(max_width=450).add_child(folium.Vega(json.load(open('prova2.json')), width=450, height=250))).add_to(m)
             

# Circle marker
folium.CircleMarker(
    location=[46.486835, 11.335177],
    radius=175,
    popup='Bolzano Area',
    color='#428bca',
    fill=True,
    fill_color='#428bca'
).add_to(m)

# Geojson overlay
#folium.GeoJson(overlay, name='cambridge').add_to(m)

# Generate map
m.save('map.html')