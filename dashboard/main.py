import dash
from dash import dcc, html
from dash import dash_table
import plotly.graph_objects as go
from dash.dependencies import Input, Output
from pymongo import MongoClient

# connect to MongoDB
client = MongoClient('mongodb://root:example@localhost:27017/admin.election_results')
db = client.get_default_database()

data = [
    ["state", "latitude", "longitude", "name"],
    ["AK", 63.588753, -154.493062, "Alaska"],
    ["AL", 32.318231, -86.902298, "Alabama"],
    ["AR", 35.20105, -91.831833, "Arkansas"],
    ["AZ", 34.048928, -111.093731, "Arizona"],
    ["CA", 36.778261, -119.417932, "California"],
    ["CO", 39.550051, -105.782067, "Colorado"],
    ["CT", 41.603221, -73.087749, "Connecticut"],
    ["DC", 38.905985, -77.033418, "District of Columbia"],
    ["DE", 38.910832, -75.52767, "Delaware"],
    ["FL", 27.664827, -81.515754, "Florida"],
    ["GA", 32.157435, -82.907123, "Georgia"],
    ["HI", 19.898682, -155.665857, "Hawaii"],
    ["IA", 41.878003, -93.097702, "Iowa"],
    ["ID", 44.068202, -114.742041, "Idaho"],
    ["IL", 40.633125, -89.398528, "Illinois"],
    ["IN", 40.551217, -85.602364, "Indiana"],
    ["KS", 39.011902, -98.484246, "Kansas"],
    ["KY", 37.839333, -84.270018, "Kentucky"],
    ["LA", 31.244823, -92.145024, "Louisiana"],
    ["MA", 42.407211, -71.382437, "Massachusetts"],
    ["MD", 39.045755, -76.641271, "Maryland"],
    ["ME", 45.253783, -69.445469, "Maine"],
    ["MI", 44.314844, -85.602364, "Michigan"],
    ["MN", 46.729553, -94.6859, "Minnesota"],
    ["MO", 37.964253, -91.831833, "Missouri"],
    ["MS", 32.354668, -89.398528, "Mississippi"],
    ["MT", 46.879682, -110.362566, "Montana"],
    ["NC", 35.759573, -79.0193, "North Carolina"],
    ["ND", 47.551493, -101.002012, "North Dakota"],
    ["NE", 41.492537, -99.901813, "Nebraska"],
    ["NH", 43.193852, -71.572395, "New Hampshire"],
    ["NJ", 40.058324, -74.405661, "New Jersey"],
    ["NM", 34.97273, -105.032363, "New Mexico"],
    ["NV", 38.80261, -116.419389, "Nevada"],
    ["NY", 43.299428, -74.217933, "New York"],
    ["OH", 40.417287, -82.907123, "Ohio"],
    ["OK", 35.007752, -97.092877, "Oklahoma"],
    ["OR", 43.804133, -120.554201, "Oregon"],
    ["PA", 41.203322, -77.194525, "Pennsylvania"],
    ["PR", 18.220833, -66.590149, "Puerto Rico"],
    ["RI", 41.580095, -71.477429, "Rhode Island"],
    ["SC", 33.836081, -81.163725, "South Carolina"],
    ["SD", 43.969515, -99.901813, "South Dakota"],
    ["TN", 35.517491, -86.580447, "Tennessee"],
    ["TX", 31.968599, -99.901813, "Texas"],
    ["UT", 39.32098, -111.093731, "Utah"],
    ["VA", 37.431573, -78.656894, "Virginia"],
    ["VT", 44.558803, -72.577841, "Vermont"],
    ["WA", 47.751074, -120.740139, "Washington"],
    ["WI", 43.78444, -88.787868, "Wisconsin"],
    ["WV", 38.597626, -80.454903, "West Virginia"],
    ["WY", 43.075968, -107.290284, "Wyoming"]
]

# create a dictionary to map state names to abbreviations
state_to_abbrev = {item[3]: item[0] for item in data[1:]}

# define Dash app
app = dash.Dash(__name__)

# build layout
app.layout = html.Div([
    dcc.Graph(id='election-map'),
])

@app.callback(
    Output('election-map', 'figure'),
    Input('election-map', 'id'),  
)
def update_map(_):
    # get election results from MongoDB
    results = list(db.election_results.find())

    # process results
    states = [state_to_abbrev[result['state']] for result in results]  
    parties = [result['party'] for result in results]
    party_numeric = [0 if party == 'Republican' else 1 for party in parties]  
    votes_percentages = [result['percentage'] for result in results]  

    # create hover text
    hover_text = [f"{party}: {percentage}%" for party, percentage in zip(parties, votes_percentages)]

        # create figure
    fig = go.Figure(data=go.Choropleth(
        locations=states,  
        z=party_numeric,  
        locationmode='USA-states',  
        colorscale=[(0, 'blue'), (1, 'red')],  
        text=hover_text,  
        marker_line_color='white',  
        showscale=False  
    ))

        
    fig.add_trace(go.Scatter(x=[None], y=[None], mode='markers', marker=dict(size=0, color='blue'), showlegend=True, name='Democrat'))
    fig.add_trace(go.Scatter(x=[None], y=[None], mode='markers', marker=dict(size=0, color='red'), showlegend=True, name='Republican'))

    # update layout
    fig.update_layout(
        title_text='US Election Results',
        geo=dict(
            scope='usa',
            projection=go.layout.geo.Projection(type='albers usa'),
            showlakes=True,  
            lakecolor='rgb(255, 255, 255)'),  
        autosize=False,
        width=1000,  
        height=800,  
        legend=dict(  
            x=0.8,
            y=0.1
        )
    )

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)