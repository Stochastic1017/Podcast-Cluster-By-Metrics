from dash import html
import dash

dash.register_page(__name__, path="/main")  # Registers this as the main app page

layout = html.Div(
    style={
        'backgroundColor': 'black',
        'height': '100vh',
        'display': 'flex',
        'flexDirection': 'column',
        'alignItems': 'center',
        'justifyContent': 'center',
    },
    children=[
        html.H1("Welcome to the Main App!", style={"color": "black"}),
        html.P("This is the main content of your web app."),
    ]
)
