from dash import html, dcc, Input, Output, callback
import dash

dash.register_page(__name__, path="/")  # Registers this as the root page

layout = html.Div(
    style={
        'backgroundColor': 'black',
        'height': '100vh',
        'display': 'flex',
        'flexDirection': 'column',
        'alignItems': 'center',
        'justifyContent': 'center',
        'opacity': '1',
        'transition': 'opacity 1s ease-in-out',
    },
    id="intro-page",
    children=[
        # Video component
        html.Video(
            id="intro-video",
            src='/assets/SpotifyLogo.mp4',
            autoPlay=True,
            controls=False,
            loop=False,
            muted=True,
            style={'width': '50%'},
        ),
        # Interval component to act as a timer
        dcc.Interval(
            id="show-button-timer",
            interval=4000,  # Time in milliseconds (5 seconds)
            n_intervals=0,  # Start at 0 intervals
            max_intervals=1,  # Stop after firing once
        ),
        html.Div(
            id="button-container",
            children=html.Button(
                "Enter App",  # Button text
                id="enter-button",  # Matches the CSS ID in styles.css
            ),
            style={'height': '80px', 'display': 'flex', 'alignItems': 'center'},
        ),
    ]
)

# Callback to show button after the timer completes
@callback(
    [Output("enter-button", "style"), Output("enter-button", "className")],
    Input("show-button-timer", "n_intervals"),
)
def show_button(n_intervals):
    if n_intervals > 0:
        return (
            {
                'marginTop': '20px',
                'padding': '10px 20px',
                'fontSize': '16px',
                'backgroundColor': '#1DB954',  # Spotify green
                'color': 'black',
                'border': 'none',
                'borderRadius': '5px',
                'cursor': 'pointer',
                'display': 'inline-block',  # Make the button visible
            },
            "fade-in",  # Add the fade-in animation class
        )
    return {'display': 'none'}, ""  # Keep hidden until timer fires

@callback(
    Output("intro-page", "style"),
    Output("url", "pathname"),
    Input("enter-button", "n_clicks"),
)
def handle_button_click(n_clicks):
    if n_clicks:
        # Fade out the intro page, then navigate to the main app
        return {'opacity': '0', 'transition': 'opacity 1s ease-in-out'}, "/main"
    return dash.no_update, dash.no_update
