import dash
from dash import dcc, html

# Create the Dash app
app = dash.Dash(
    __name__,
    use_pages=True,  # Automatically detects pages in the pages/ folder
    suppress_callback_exceptions=True,  # Allow callbacks for dynamically generated layouts
)

# App layout
app.layout = html.Div(
    children=[
        dcc.Location(id="url"),  # Tracks current page
        dash.page_container,  # Automatically displays content of the current page
    ]
)

if __name__ == "__main__":
    app.run_server(debug=True)
