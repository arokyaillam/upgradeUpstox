"""
🐋 WHALE HUNTER DASHBOARD
Real-time Nifty Options Trading Analytics

🚀 WebSocket + Redis Pub/Sub - NO POLLING!

Flow:
    Signal Generator → Redis Pub/Sub → FastAPI WebSocket → Dash → UI Update

Run: uv run python app/dashboard/app.py
"""

import os
import sys
import json
import requests
from datetime import datetime
from typing import Dict, Any, List

import dash
from dash import html, dcc, callback, Input, Output, State, clientside_callback
from dash.exceptions import PreventUpdate
from dash_extensions import WebSocket
import plotly.graph_objects as go

# Add parent to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# =============================================================================
# CONFIGURATION
# =============================================================================

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8001")
WS_URL = os.getenv("WS_URL", "ws://localhost:8001/ws/signals")
HISTORY_URL = f"{API_BASE_URL}/dashboard/history"

# =============================================================================
# DASH APP INITIALIZATION
# =============================================================================

app = dash.Dash(
    __name__,
    title="🐋 Whale Hunter",
    update_title=None,
    suppress_callback_exceptions=True,
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"}
    ]
)

# =============================================================================
# STYLES
# =============================================================================

COLORS = {
    "bg_dark": "#0a0a0f",
    "bg_card": "#12121a",
    "bg_table_header": "#1a1a2e",
    "bg_table_row": "#0f0f18",
    "bg_table_row_alt": "#14141f",
    "border": "#2a2a3a",
    "text_primary": "#ffffff",
    "text_secondary": "#8888aa",
    "accent_green": "#00ff88",
    "accent_red": "#ff4466",
    "accent_yellow": "#ffcc00",
    "accent_blue": "#00aaff",
    "accent_purple": "#aa66ff",
    "accent_orange": "#ff8844",
}

CARD_STYLE = {
    "backgroundColor": COLORS["bg_card"],
    "borderRadius": "12px",
    "border": f"1px solid {COLORS['border']}",
    "padding": "20px",
    "marginBottom": "16px",
}

TABLE_HEADER_STYLE = {
    "backgroundColor": COLORS["bg_table_header"],
    "padding": "12px 8px",
    "textAlign": "left",
    "color": COLORS["text_secondary"],
    "fontSize": "11px",
    "textTransform": "uppercase",
    "letterSpacing": "0.5px",
    "fontWeight": "600",
    "borderBottom": f"2px solid {COLORS['border']}",
}

TABLE_CELL_STYLE = {
    "padding": "10px 8px",
    "fontSize": "12px",
    "borderBottom": f"1px solid {COLORS['border']}",
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_sentiment_color(score: float) -> str:
    if score >= 60: return COLORS["accent_green"]
    elif score >= 20: return "#66ff99"
    elif score <= -60: return COLORS["accent_red"]
    elif score <= -20: return "#ff6688"
    return COLORS["accent_yellow"]


def get_pattern_color(pattern: str) -> str:
    pattern_colors = {
        "Long Buildup": COLORS["accent_green"],
        "Short Covering": COLORS["accent_green"],
        "Panic (Short Covering)": COLORS["accent_green"],
        "Short Buildup": COLORS["accent_red"],
        "Long Unwinding": COLORS["accent_red"],
        "Neutral": COLORS["accent_yellow"],
        "Low Volume": COLORS["text_secondary"],
    }
    return pattern_colors.get(pattern, COLORS["text_secondary"])


def get_signal_color(signal: str) -> str:
    signal = str(signal)
    if any(x in signal for x in ["Bullish", "BUY", "🚀", "🔥"]):
        return COLORS["accent_green"]
    elif any(x in signal for x in ["Bearish", "SELL"]):
        return COLORS["accent_red"]
    return COLORS["text_secondary"]


def format_timestamp(ts) -> str:
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            return dt.strftime('%H:%M:%S')
        except:
            return ts[:8] if len(ts) >= 8 else ts
    return str(ts)[:8]


def format_number(val, decimals=2) -> str:
    if val is None: return "-"
    try:
        num = float(val)
        if decimals == 0:
            return f"{int(num):+,}" if num != 0 else "0"
        return f"{num:+,.{decimals}f}" if num != 0 else f"{num:.{decimals}f}"
    except:
        return str(val)



def split_and_filter_data(data: List[Dict], limit: int = 10):
    """Split data into Calls and Puts, filtering for unique instruments."""
    calls = []
    puts = []
    seen_ce = set()
    seen_pe = set()
    
    for row in data:
        # Ensure key is a string and handle None
        key = str(row.get('instrument_key', ''))
        
        # Check for CE/PE in key (e.g., "NIFTY23OCT19000CE" or "24100_CE")
        if '_CE' in key or ('CE' in key and 'PE' not in key):
            if key not in seen_ce and len(calls) < limit:
                seen_ce.add(key)
                calls.append(row)
        elif '_PE' in key or ('PE' in key and 'CE' not in key):
            if key not in seen_pe and len(puts) < limit:
                seen_pe.add(key)
                puts.append(row)
                
    return calls, puts


# =============================================================================
# TABLE GENERATORS
# =============================================================================

def create_table(data: List[Dict], columns: List[Dict], empty_msg: str = "No data") -> html.Div:
    """Generic table creator."""
    if not data:
        return html.Div(empty_msg, style={
            "color": COLORS["text_secondary"], 
            "padding": "20px", 
            "textAlign": "center"
        })
    
    headers = [col["header"] for col in columns]
    
    rows = []
    for i, row in enumerate(data[:10]):
        bg_color = COLORS["bg_table_row"] if i % 2 == 0 else COLORS["bg_table_row_alt"]
        cells = []
        for col in columns:
            value = row.get(col["field"], "-")
            style = {**TABLE_CELL_STYLE}
            
            # Apply formatters
            if col.get("format") == "time":
                value = format_timestamp(value)
                style["color"] = COLORS["text_secondary"]
            elif col.get("format") == "number":
                value = format_number(value, col.get("decimals", 2))
                style["textAlign"] = "right"
            elif col.get("format") == "int":
                value = format_number(value, 0)
                style["textAlign"] = "right"
            elif col.get("format") == "pattern":
                style["color"] = get_pattern_color(str(value))
            elif col.get("format") == "signal":
                style["color"] = get_signal_color(str(value))
            elif col.get("format") == "sentiment":
                style["color"] = get_sentiment_color(float(row.get("sentiment_score", 0)))
            elif col.get("format") == "score":
                score = float(value) if value else 0
                style["color"] = COLORS["accent_green"] if score > 65 else COLORS["accent_red"] if score < 35 else COLORS["accent_yellow"]
                style["fontWeight"] = "bold"
                style["textAlign"] = "center"
            elif col.get("format") == "ratio":
                ratio = float(value) if value else 0
                style["color"] = COLORS["accent_green"] if ratio > 0.2 else COLORS["accent_red"] if ratio < -0.2 else COLORS["text_secondary"]
                style["fontWeight"] = "bold"
                style["textAlign"] = "right"
                value = f"{ratio:+.3f}"
            elif col.get("format") == "currency":
                value = f"₹{float(value):,.2f}" if value else "-"
            elif col.get("format") == "percent":
                value = f"{float(value):+.2f}%" if value else "-"
                style["color"] = COLORS["accent_green"] if float(row.get(col["field"], 0) or 0) > 0 else COLORS["accent_red"]
            elif col.get("format") == "whale":
                whale_type = str(value)
                icon = "🐋" if "Mega" in whale_type else "🐳" if "Large" in whale_type else "🐬"
                value = f"{icon} {whale_type}"
                style["color"] = COLORS["accent_blue"]
            
            if col.get("bold"):
                style["fontWeight"] = "bold"
            if col.get("truncate"):
                value = str(value)[:col["truncate"]]
            
            cells.append(html.Td(value, style=style))
        
        rows.append(html.Tr(cells, style={"backgroundColor": bg_color}))
    
    return html.Table([
        html.Thead(html.Tr([html.Th(h, style=TABLE_HEADER_STYLE) for h in headers])),
        html.Tbody(rows)
    ], style={"width": "100%", "borderCollapse": "collapse"})


# Table column definitions
PATTERNS_COLS = [
    {"header": "Time", "field": "timestamp", "format": "time"},
    {"header": "Instrument", "field": "instrument_key", "bold": True, "truncate": 12},
    {"header": "Pattern", "field": "pattern", "format": "pattern"},
    {"header": "Signal", "field": "signal", "format": "signal"},
    {"header": "OI Δ", "field": "oi_change", "format": "int"},
]

PANIC_COLS = [
    {"header": "Time", "field": "timestamp", "format": "time"},
    {"header": "Instrument", "field": "instrument_key", "bold": True, "truncate": 12},
    {"header": "Signal", "field": "signal", "format": "signal"},
    {"header": "Price %", "field": "price_change_pct", "format": "percent"},
    {"header": "OI Δ", "field": "oi_change", "format": "int"},
]

IMBALANCE_COLS = [
    {"header": "Time", "field": "timestamp", "format": "time"},
    {"header": "Instrument", "field": "instrument_key", "bold": True, "truncate": 12},
    {"header": "TBQ", "field": "tbq", "format": "int"},
    {"header": "TSQ", "field": "tsq", "format": "int"},
    {"header": "Ratio", "field": "imbalance_ratio", "format": "ratio"},
    {"header": "Signal", "field": "signal", "format": "signal"},
]

GREEKS_COLS = [
    {"header": "Time", "field": "timestamp", "format": "time"},
    {"header": "Instrument", "field": "instrument_key", "bold": True, "truncate": 12},
    {"header": "Score", "field": "momentum_score", "format": "score"},
    {"header": "Type", "field": "momentum_type", "truncate": 15},
    {"header": "Signal", "field": "signal", "format": "signal"},
]

WHALE_COLS = [
    {"header": "Time", "field": "timestamp", "format": "time"},
    {"header": "Instrument", "field": "instrument_key", "bold": True, "truncate": 12},
    {"header": "Whale", "field": "whale_type", "format": "whale"},
    {"header": "Alert", "field": "alert_type"},
    {"header": "Value", "field": "alert_value", "format": "int"},
]

SENTIMENT_COLS = [
    {"header": "Time", "field": "timestamp", "format": "time"},
    {"header": "Instrument", "field": "instrument_key", "bold": True, "truncate": 12},
    {"header": "Sentiment", "field": "sentiment", "format": "sentiment"},
    {"header": "Score", "field": "sentiment_score", "format": "score"},
    {"header": "Regime", "field": "market_regime", "truncate": 18},
]


def create_sentiment_gauge(score: float, sentiment: str) -> go.Figure:
    """Create sentiment gauge."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': sentiment, 'font': {'size': 14, 'color': COLORS["text_primary"]}},
        number={'font': {'size': 32, 'color': get_sentiment_color(score)}},
        gauge={
            'axis': {'range': [-100, 100], 'tickcolor': COLORS["text_secondary"]},
            'bar': {'color': get_sentiment_color(score)},
            'bgcolor': COLORS["bg_dark"],
            'borderwidth': 0,
            'steps': [
                {'range': [-100, -60], 'color': 'rgba(255,68,102,0.3)'},
                {'range': [-60, -20], 'color': 'rgba(255,102,136,0.2)'},
                {'range': [-20, 20], 'color': 'rgba(255,204,0,0.2)'},
                {'range': [20, 60], 'color': 'rgba(102,255,153,0.2)'},
                {'range': [60, 100], 'color': 'rgba(0,255,136,0.3)'},
            ],
        }
    ))
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'color': COLORS["text_primary"]},
        height=180,
        margin=dict(l=20, r=20, t=30, b=10)
    )
    return fig


def create_call_put_chart(data: Dict[str, List[Dict]]) -> go.Figure:
    """Create Call vs Put buy signals comparison chart with 5 metrics (Unique instruments only)."""
    metrics = ['Patterns', 'Panic', 'Imbalance', 'Greeks', 'Whale']

    call_counts = []
    put_counts = []

    # Helper to check if signal is a buy signal
    def is_buy_signal(signal_str: str) -> bool:
        signal = str(signal_str).upper()
        return any(x in signal for x in ['BULLISH', 'BUY', 'STRONG BUY', '🚀', '🔥'])

    # Helper to count unique CE/PE instrument keys with buy signals
    def count_signals(data_list: List[Dict], limit: int = 20) -> tuple:
        ce_keys = set()
        pe_keys = set()
        # Take only the latest N records
        latest_data = data_list[:limit] if data_list else []

        for item in latest_data:
            key = str(item.get('instrument_key', ''))
            signal = item.get('signal', '')
            if is_buy_signal(signal):
                if '_CE' in key or ('CE' in key and 'PE' not in key):
                    ce_keys.add(key)
                elif '_PE' in key or ('PE' in key and 'CE' not in key):
                    pe_keys.add(key)

        return len(ce_keys), len(pe_keys)

    # Calculate metrics from latest data
    patterns_data = data.get('patterns', [])
    ce, pe = count_signals(patterns_data, limit=20)
    call_counts.append(ce)
    put_counts.append(pe)

    panic_data = data.get('panic', [])
    ce, pe = count_signals(panic_data, limit=20)
    call_counts.append(ce)
    put_counts.append(pe)

    imbalance_data = data.get('imbalance', [])
    ce, pe = count_signals(imbalance_data, limit=20)
    call_counts.append(ce)
    put_counts.append(pe)

    greeks_data = data.get('greeks', [])
    ce, pe = count_signals(greeks_data, limit=20)
    call_counts.append(ce)
    put_counts.append(pe)

    whale_data = data.get('whales', [])
    ce, pe = count_signals(whale_data, limit=20)
    call_counts.append(ce)
    put_counts.append(pe)

    # Create grouped bar chart
    fig = go.Figure(data=[
        go.Bar(
            name='Calls (CE)',
            x=metrics,
            y=call_counts,
            marker_color=COLORS["accent_green"],
            text=call_counts,
            textposition='outside',
            textfont={'size': 10}
        ),
        go.Bar(
            name='Puts (PE)',
            x=metrics,
            y=put_counts,
            marker_color=COLORS["accent_red"],
            text=put_counts,
            textposition='outside',
            textfont={'size': 10}
        )
    ])

    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'color': COLORS["text_primary"], 'size': 10},
        height=180,
        margin=dict(l=20, r=20, t=10, b=40),
        xaxis={
            'tickfont': {'size': 10},
            'fixedrange': True,
            'showgrid': False
        },
        yaxis={
            'showgrid': True,
            'gridcolor': COLORS["border"],
            'fixedrange': True,
            'title': 'Unique Instruments'
        },
        barmode='group',
        bargap=0.15,
        bargroupgap=0.1,
        legend={
            'orientation': 'h',
            'yanchor': 'bottom',
            'y': 1.02,
            'xanchor': 'right',
            'x': 1,
            'font': {'size': 9}
        },
        autosize=True
    )
    return fig


def create_split_section(title: str, icon: str, base_id: str, color: str = None):
    """Create a section card with split Call/Put tables."""
    return html.Div([
        # Header
        html.Div([
            html.Span(icon, style={"fontSize": "18px", "marginRight": "10px"}),
            html.H3(title, style={
                "color": color or COLORS["text_primary"],
                "margin": "0", "fontSize": "14px", "fontWeight": "600"
            }),
            html.Div(id=f"{base_id}-count", style={
                "marginLeft": "auto",
                "backgroundColor": COLORS["bg_dark"],
                "padding": "4px 10px",
                "borderRadius": "12px",
                "fontSize": "11px",
                "color": COLORS["text_secondary"]
            })
        ], style={
            "display": "flex", "alignItems": "center",
            "marginBottom": "16px", "paddingBottom": "12px",
            "borderBottom": f"1px solid {COLORS['border']}"
        }),
        
        # Split Content
        html.Div([
            # CALLS
            html.Div([
                html.Div("CALLS (CE)", style={
                    "color": COLORS["accent_green"], "fontSize": "12px", "fontWeight": "bold", "marginBottom": "8px", "textAlign": "center",
                    "borderBottom": f"1px solid {COLORS['accent_green']}", "paddingBottom": "4px"
                }),
                html.Div(id=f"{base_id}-table-ce")
            ], style={"flex": "1", "marginRight": "12px"}),
            
            # PUTS
            html.Div([
                html.Div("PUTS (PE)", style={
                    "color": COLORS["accent_red"], "fontSize": "12px", "fontWeight": "bold", "marginBottom": "8px", "textAlign": "center",
                    "borderBottom": f"1px solid {COLORS['accent_red']}", "paddingBottom": "4px"
                }),
                html.Div(id=f"{base_id}-table-pe")
            ], style={"flex": "1"})
        ], style={"display": "flex"})
        
    ], style={**CARD_STYLE, "minHeight": "300px"})


# =============================================================================
# MAIN LAYOUT
# =============================================================================

app.layout = html.Div([
    # WebSocket Connection (NO POLLING!)
    WebSocket(id="ws", url=WS_URL),
    
    # Data Store - Initialize with history if available
    dcc.Store(id='metrics-store', data={}),
    # Interval to fetch history once on load
    dcc.Interval(id='init-interval', interval=1000, n_intervals=0, max_intervals=1),
    
    # Header
    html.Div([
        html.Div([
            html.H1("🐋 WHALE HUNTER", style={
                "margin": "0", "fontSize": "24px", "fontWeight": "bold"
            }),
            html.Span("Real-time • WebSocket", style={
                "color": COLORS["text_secondary"],
                "fontSize": "12px", "marginLeft": "12px"
            })
        ], style={"display": "flex", "alignItems": "baseline"}),

        html.Div([
            html.Button("⚙️ Settings", id="settings-btn", n_clicks=0, style={
                "backgroundColor": COLORS["bg_dark"],
                "color": COLORS["text_primary"],
                "border": f"1px solid {COLORS['border']}",
                "borderRadius": "6px",
                "padding": "6px 12px",
                "fontSize": "12px",
                "cursor": "pointer",
                "marginRight": "16px"
            }),
            html.Div(id="ws-status", children=[
                html.Span("●", style={"color": COLORS["accent_yellow"], "marginRight": "6px"}),
                html.Span("Connecting...", style={"fontSize": "12px"})
            ]),
            html.Div(id="last-update", style={
                "color": COLORS["text_secondary"],
                "fontSize": "11px", "marginLeft": "16px"
            })
        ], style={"display": "flex", "alignItems": "center"})
    ], style={
        "display": "flex", "justifyContent": "space-between", "alignItems": "center",
        "padding": "16px 20px", "backgroundColor": COLORS["bg_card"],
        "borderBottom": f"1px solid {COLORS['border']}",
        "position": "sticky", "top": "0", "zIndex": "100"
    }),

    # Settings Panel (Collapsible)
    html.Div(id="settings-panel", children=[
        html.Div([
            # Token Generation Section
            html.Div([
                html.H3("🔐 Token Generation", style={"fontSize": "14px", "marginBottom": "12px", "color": COLORS["accent_blue"]}),
                html.Div([
                    html.Div([
                        html.Label("API Key:", style={"fontSize": "11px", "color": COLORS["text_secondary"], "marginBottom": "4px"}),
                        dcc.Input(id="api-key-input", type="text", value="", placeholder="Enter Upstox API Key", style={
                            "width": "100%", "padding": "8px", "backgroundColor": COLORS["bg_dark"],
                            "border": f"1px solid {COLORS['border']}", "borderRadius": "4px",
                            "color": COLORS["text_primary"], "fontSize": "12px"
                        })
                    ], style={"marginBottom": "10px"}),
                    html.Div([
                        html.Label("API Secret:", style={"fontSize": "11px", "color": COLORS["text_secondary"], "marginBottom": "4px"}),
                        dcc.Input(id="api-secret-input", type="password", value="", placeholder="Enter Upstox API Secret", style={
                            "width": "100%", "padding": "8px", "backgroundColor": COLORS["bg_dark"],
                            "border": f"1px solid {COLORS['border']}", "borderRadius": "4px",
                            "color": COLORS["text_primary"], "fontSize": "12px"
                        })
                    ], style={"marginBottom": "10px"}),
                    html.Button("Save Credentials", id="generate-token-btn", n_clicks=0, style={
                        "backgroundColor": COLORS["accent_blue"],
                        "color": COLORS["text_primary"],
                        "border": "none",
                        "borderRadius": "4px",
                        "padding": "8px 16px",
                        "fontSize": "12px",
                        "cursor": "pointer",
                        "fontWeight": "600"
                    }),
                    html.Div(id="token-status", style={"marginTop": "8px", "fontSize": "11px"}),
                    html.Div([
                        html.Div("📌 Next Step:", style={"fontSize": "11px", "fontWeight": "bold", "color": COLORS["text_secondary"], "marginTop": "12px", "marginBottom": "6px"}),
                        html.Div("After saving credentials, run this command in terminal:", style={"fontSize": "10px", "color": COLORS["text_secondary"], "marginBottom": "4px"}),
                        html.Code("python get_token.py", style={
                            "backgroundColor": COLORS["bg_dark"],
                            "padding": "4px 8px",
                            "borderRadius": "3px",
                            "fontSize": "10px",
                            "color": COLORS["accent_green"],
                            "border": f"1px solid {COLORS['border']}"
                        }),
                        html.Div("⚠️ Browser will open on port 8000 for Upstox login", style={"fontSize": "10px", "color": COLORS["accent_yellow"], "marginTop": "6px"})
                    ])
                ])
            ], style={"flex": "1", "marginRight": "16px"}),

            # Ingestion Configuration Section
            html.Div([
                html.H3("📡 Ingestion Config", style={"fontSize": "14px", "marginBottom": "12px", "color": COLORS["accent_green"]}),
                html.Div([
                    html.Div([
                        html.Label("Expiry Date:", style={"fontSize": "11px", "color": COLORS["text_secondary"], "marginBottom": "4px"}),
                        dcc.Input(id="expiry-date-input", type="text", value="", placeholder="YYYY-MM-DD (e.g. 2025-11-27)", style={
                            "width": "100%", "padding": "8px", "backgroundColor": COLORS["bg_dark"],
                            "border": f"1px solid {COLORS['border']}", "borderRadius": "4px",
                            "color": COLORS["text_primary"], "fontSize": "12px"
                        })
                    ], style={"marginBottom": "10px"}),
                    html.Div([
                        html.Label("Strike Price (ATM):", style={"fontSize": "11px", "color": COLORS["text_secondary"], "marginBottom": "4px"}),
                        dcc.Input(id="strike-price-input", type="number", value="", placeholder="e.g. 24000", style={
                            "width": "100%", "padding": "8px", "backgroundColor": COLORS["bg_dark"],
                            "border": f"1px solid {COLORS['border']}", "borderRadius": "4px",
                            "color": COLORS["text_primary"], "fontSize": "12px"
                        })
                    ], style={"marginBottom": "10px"}),
                    html.Button("Start Ingestion", id="start-ingestion-btn", n_clicks=0, style={
                        "backgroundColor": COLORS["accent_green"],
                        "color": COLORS["text_primary"],
                        "border": "none",
                        "borderRadius": "4px",
                        "padding": "8px 16px",
                        "fontSize": "12px",
                        "cursor": "pointer",
                        "fontWeight": "600",
                        "marginRight": "8px"
                    }),
                    html.Button("Stop Ingestion", id="stop-ingestion-btn", n_clicks=0, style={
                        "backgroundColor": COLORS["accent_red"],
                        "color": COLORS["text_primary"],
                        "border": "none",
                        "borderRadius": "4px",
                        "padding": "8px 16px",
                        "fontSize": "12px",
                        "cursor": "pointer",
                        "fontWeight": "600"
                    }),
                    html.Div(id="ingestion-status", style={"marginTop": "8px", "fontSize": "11px"})
                ])
            ], style={"flex": "1"})
        ], style={"display": "flex"})
    ], style={
        **CARD_STYLE,
        "margin": "16px",
        "display": "none"  # Hidden by default
    }),
    
    # Main Content
    html.Div([
        # Row 1
        html.Div([
            html.Div([
                html.Div("📊 Sentiment", style={"fontSize": "13px", "marginBottom": "10px"}),
                dcc.Graph(id='sentiment-gauge', config={'displayModeBar': False}, style={'height': '200px'})
            ], style={**CARD_STYLE, "flex": "0.8", "marginRight": "12px", "padding": "15px"}),
            
            html.Div([
                html.Div("📈 Call vs Put Buy Signals (Unique)", style={"fontSize": "13px", "marginBottom": "10px"}),
                dcc.Graph(id='pattern-chart', config={'displayModeBar': False}, style={'height': '200px'})
            ], style={**CARD_STYLE, "flex": "1.5", "marginRight": "12px", "padding": "15px"}),
            
            html.Div([
                html.Div("⚡ Quick Stats", style={"fontSize": "13px", "marginBottom": "15px"}),
                html.Div([
                    html.Div([
                        html.Div("Patterns", style={"color": COLORS["text_secondary"], "fontSize": "10px"}),
                        html.Div("--", id='stat-patterns', style={"fontSize": "22px", "fontWeight": "bold", "color": COLORS["accent_blue"]})
                    ], style={"textAlign": "center", "flex": "1"}),
                    html.Div([
                        html.Div("Panic", style={"color": COLORS["text_secondary"], "fontSize": "10px"}),
                        html.Div("--", id='stat-panic', style={"fontSize": "22px", "fontWeight": "bold", "color": COLORS["accent_red"]})
                    ], style={"textAlign": "center", "flex": "1"}),
                    html.Div([
                        html.Div("Whales", style={"color": COLORS["text_secondary"], "fontSize": "10px"}),
                        html.Div("--", id='stat-whales', style={"fontSize": "22px", "fontWeight": "bold", "color": COLORS["accent_purple"]})
                    ], style={"textAlign": "center", "flex": "1"}),
                ], style={"display": "flex"})
            ], style={**CARD_STYLE, "flex": "0.8", "padding": "15px"})
        ], style={"display": "flex", "marginBottom": "12px"}),
        
        # Row 2
        # Row 2: Patterns
        create_split_section("Market Patterns", "📊", "patterns", COLORS["accent_blue"]),
        
        # Row 3: Panic
        create_split_section("Panic Signals", "🚨", "panic", COLORS["accent_red"]),
        
        # Row 4: Imbalance
        create_split_section("Order Imbalance", "📖", "imbalance", COLORS["accent_orange"]),
        
        # Row 5: Greeks
        create_split_section("Greeks Momentum", "Δ", "greeks", COLORS["accent_purple"]),
        
        # Row 6: Whales
        create_split_section("Whale Activity", "🐋", "whale", COLORS["accent_blue"]),
        
        # Row 7: Sentiment
        create_split_section("Market Sentiment", "🎯", "sentiment", COLORS["accent_green"]),
        
    ], style={"padding": "16px"})
    
], style={
    "backgroundColor": COLORS["bg_dark"],
    "minHeight": "100vh",
    "fontFamily": "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
    "color": COLORS["text_primary"]
})


# =============================================================================
# CALLBACKS - WebSocket Based (NO POLLING!)
# =============================================================================

@callback(
    Output('metrics-store', 'data'),
    Output('ws-status', 'children'),
    Output('last-update', 'children'),
    Input('ws', 'message'),
    Input('init-interval', 'n_intervals'),
    State('metrics-store', 'data')
)
def process_updates(message, n_intervals, current_data):
    """Process incoming WebSocket messages and initial history load."""
    ctx = dash.callback_context
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    # 1. Initial Load (History)
    if trigger_id == 'init-interval':
        try:
            print(f"🔄 Fetching history from {HISTORY_URL}...")
            response = requests.get(HISTORY_URL, timeout=5)
            if response.status_code == 200:
                history = response.json()
                print(f"✅ History loaded: {len(history.get('patterns', []))} patterns")
                
                # Calculate initial summary stats
                if history:
                    # Calculate pattern summary
                    pattern_counts = {}
                    for p in history.get('patterns', []):
                        pat = p.get('pattern', 'Unknown')
                        pattern_counts[pat] = pattern_counts.get(pat, 0) + 1
                    history['pattern_summary'] = pattern_counts
                    
                    # Calculate average sentiment
                    sentiments = [float(s.get('sentiment_score', 0)) for s in history.get('sentiment', [])]
                    history['avg_sentiment'] = sum(sentiments) / len(sentiments) if sentiments else 0
                    
                return history, dash.no_update, "Loaded History"
        except Exception as e:
            print(f"❌ Failed to load history: {e}")
            return dash.no_update, dash.no_update, dash.no_update

    # 2. WebSocket Updates
    if message is None:
        raise PreventUpdate

    try:
        data = json.loads(message['data'])
        msg_type = data.get('type', 'pattern')

        new_metrics = current_data or {}

        if msg_type in ['initial', 'refresh']:
            # Full data update
            new_metrics = data.get('data', {})
        elif msg_type == 'pong':
            raise PreventUpdate
        else:
            # Single signal update - Route based on type
            # Helper to prepend and limit
            def update_list(key, item, limit=50):
                if key not in new_metrics: new_metrics[key] = []
                new_metrics[key].insert(0, item)
                new_metrics[key] = new_metrics[key][:limit]

            # Route based on message type
            if msg_type == 'pattern':
                update_list('patterns', data)
                # Update pattern summary
                pattern_counts = new_metrics.get('pattern_summary', {})
                pat = data.get('pattern', 'Unknown')
                pattern_counts[pat] = pattern_counts.get(pat, 0) + 1
                new_metrics['pattern_summary'] = pattern_counts
            elif msg_type == 'panic':
                update_list('panic', data)
            elif msg_type == 'imbalance':
                update_list('imbalance', data)
            elif msg_type == 'greeks':
                update_list('greeks', data)
            elif msg_type == 'whale':
                update_list('whales', data)
            elif msg_type == 'sentiment':
                update_list('sentiment', data)
                # Update average sentiment
                sentiments = [float(s.get('sentiment_score', 0)) for s in new_metrics.get('sentiment', [])]
                new_metrics['avg_sentiment'] = sum(sentiments) / len(sentiments) if sentiments else 0

        
        status = [
            html.Span("●", style={"color": COLORS["accent_green"], "marginRight": "6px", "animation": "pulse 1s infinite"}),
            html.Span("Live", style={"color": COLORS["accent_green"], "fontSize": "12px"})
        ]
        
        timestamp = datetime.now().isoformat()
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            update_text = f"Updated: {dt.strftime('%H:%M:%S')}"
        except:
            update_text = f"Updated: {datetime.now().strftime('%H:%M:%S')}"
        
        return new_metrics, status, update_text
        
    except Exception as e:
        print(f"WebSocket message error: {e}")
        raise PreventUpdate


@callback(
    Output('ws-status', 'children', allow_duplicate=True),
    Input('ws', 'state'),
    prevent_initial_call=True
)
def update_ws_state(state):
    """Update WebSocket connection status."""
    if state is None:
        raise PreventUpdate
    
    if state.get('readyState') == 1:  # OPEN
        return [
            html.Span("●", style={"color": COLORS["accent_green"], "marginRight": "6px"}),
            html.Span("Connected", style={"color": COLORS["accent_green"], "fontSize": "12px"})
        ]
    elif state.get('readyState') == 0:  # CONNECTING
        return [
            html.Span("●", style={"color": COLORS["accent_yellow"], "marginRight": "6px"}),
            html.Span("Connecting...", style={"color": COLORS["accent_yellow"], "fontSize": "12px"})
        ]
    else:  # CLOSED or CLOSING
        return [
            html.Span("●", style={"color": COLORS["accent_red"], "marginRight": "6px"}),
            html.Span("Disconnected", style={"color": COLORS["accent_red"], "fontSize": "12px"})
        ]


# Summary components
@callback(
    Output('sentiment-gauge', 'figure'),
    Output('pattern-chart', 'figure'),
    Output('stat-patterns', 'children'),
    Output('stat-panic', 'children'),
    Output('stat-whales', 'children'),
    Input('metrics-store', 'data')
)
def update_summary(data):
    try:
        if not data:
            return create_sentiment_gauge(0, "No Data"), create_call_put_chart({}), "--", "--", "--"

        avg_score = data.get('avg_sentiment', 0)
        sentiment_label = "Bullish" if avg_score > 20 else "Bearish" if avg_score < -20 else "Neutral"

        pattern_summary = data.get('pattern_summary', {})
        total_patterns = sum(pattern_summary.values()) if pattern_summary else 0
        panic_count = len(data.get('panic', []))
        whale_count = len(data.get('whales', []))

        return (
            create_sentiment_gauge(avg_score, sentiment_label),
            create_call_put_chart(data),
            str(total_patterns),
            str(panic_count),
            str(whale_count)
        )
    except Exception as e:
        print(f"Error in update_summary: {e}")
        import traceback
        traceback.print_exc()
        return create_sentiment_gauge(0, "Error"), create_call_put_chart({}), "--", "--", "--"


# Table update callbacks
@callback(
    Output('patterns-table-ce', 'children'),
    Output('patterns-table-pe', 'children'),
    Output('patterns-count', 'children'),
    Input('metrics-store', 'data')
)
def update_patterns_tables(data):
    try:
        if not data:
            return create_table([], PATTERNS_COLS), create_table([], PATTERNS_COLS), "0 signals"

        patterns_data = data.get('patterns', [])
        calls, puts = split_and_filter_data(patterns_data, limit=10)

        return (
            create_table(calls, PATTERNS_COLS, "No Call patterns"),
            create_table(puts, PATTERNS_COLS, "No Put patterns"),
            f"{len(patterns_data)} signals"
        )
    except Exception as e:
        print(f"Error in update_patterns_tables: {e}")
        return create_table([], PATTERNS_COLS), create_table([], PATTERNS_COLS), "Error"


@callback(
    Output('panic-table-ce', 'children'),
    Output('panic-table-pe', 'children'),
    Output('panic-count', 'children'),
    Input('metrics-store', 'data')
)
def update_panic_tables(data):
    if not data:
        return create_table([], PANIC_COLS), create_table([], PANIC_COLS), "0 signals"

    panic_data = data.get('panic', [])
    calls, puts = split_and_filter_data(panic_data, limit=10)

    return (
        create_table(calls, PANIC_COLS, "No Call panic"),
        create_table(puts, PANIC_COLS, "No Put panic"),
        f"{len(panic_data)} signals"
    )


@callback(
    Output('imbalance-table-ce', 'children'),
    Output('imbalance-table-pe', 'children'),
    Output('imbalance-count', 'children'),
    Input('metrics-store', 'data')
)
def update_imbalance_tables(data):
    if not data:
        return create_table([], IMBALANCE_COLS), create_table([], IMBALANCE_COLS), "0 signals"

    imbalance_data = data.get('imbalance', [])
    calls, puts = split_and_filter_data(imbalance_data, limit=10)

    return (
        create_table(calls, IMBALANCE_COLS, "No Call imbalance"),
        create_table(puts, IMBALANCE_COLS, "No Put imbalance"),
        f"{len(imbalance_data)} signals"
    )


@callback(
    Output('greeks-table-ce', 'children'),
    Output('greeks-table-pe', 'children'),
    Output('greeks-count', 'children'),
    Input('metrics-store', 'data')
)
def update_greeks_tables(data):
    if not data:
        return create_table([], GREEKS_COLS), create_table([], GREEKS_COLS), "0 signals"

    greeks_data = data.get('greeks', [])
    calls, puts = split_and_filter_data(greeks_data, limit=10)

    return (
        create_table(calls, GREEKS_COLS, "No Call greeks"),
        create_table(puts, GREEKS_COLS, "No Put greeks"),
        f"{len(greeks_data)} signals"
    )


@callback(
    Output('whale-table-ce', 'children'),
    Output('whale-table-pe', 'children'),
    Output('whale-count', 'children'),
    Input('metrics-store', 'data')
)
def update_whale_tables(data):
    if not data:
        return create_table([], WHALE_COLS), create_table([], WHALE_COLS), "0 signals"

    whale_data = data.get('whales', [])
    calls, puts = split_and_filter_data(whale_data, limit=10)

    return (
        create_table(calls, WHALE_COLS, "No Call whales"),
        create_table(puts, WHALE_COLS, "No Put whales"),
        f"{len(whale_data)} signals"
    )


@callback(
    Output('sentiment-table-ce', 'children'),
    Output('sentiment-table-pe', 'children'),
    Output('sentiment-count', 'children'),
    Input('metrics-store', 'data')
)
def update_sentiment_tables(data):
    if not data:
        return create_table([], SENTIMENT_COLS), create_table([], SENTIMENT_COLS), "0 signals"

    sentiment_data = data.get('sentiment', [])
    calls, puts = split_and_filter_data(sentiment_data, limit=10)

    return (
        create_table(calls, SENTIMENT_COLS, "No Call sentiment"),
        create_table(puts, SENTIMENT_COLS, "No Put sentiment"),
        f"{len(sentiment_data)} signals"
    )


# Settings panel toggle
@callback(
    Output('settings-panel', 'style'),
    Input('settings-btn', 'n_clicks'),
    prevent_initial_call=True
)
def toggle_settings(n_clicks):
    if n_clicks and n_clicks % 2 == 1:
        # Show settings panel
        return {**CARD_STYLE, "margin": "16px", "display": "block"}
    else:
        # Hide settings panel
        return {**CARD_STYLE, "margin": "16px", "display": "none"}


# Save credentials handler
@callback(
    Output('token-status', 'children'),
    Input('generate-token-btn', 'n_clicks'),
    State('api-key-input', 'value'),
    State('api-secret-input', 'value'),
    prevent_initial_call=True
)
def save_credentials(n_clicks, api_key, api_secret):
    if not api_key or not api_secret:
        return html.Span("❌ Please enter API Key and Secret", style={"color": COLORS["accent_red"]})

    try:
        # Call API endpoint to save credentials
        print(f"Calling API: {API_BASE_URL}/save-credentials")
        response = requests.post(f"{API_BASE_URL}/save-credentials", json={
            "api_key": api_key,
            "api_secret": api_secret
        }, timeout=10)

        print(f"Response status: {response.status_code}")
        if response.status_code == 200:
            return html.Div([
                html.Span("✅ Credentials saved to .env file!", style={"color": COLORS["accent_green"], "display": "block", "marginBottom": "6px"}),
                html.Span("▶️ Now run: ", style={"color": COLORS["text_secondary"], "fontSize": "10px"}),
                html.Code("cd upgradeUpstox && python get_token.py", style={
                    "backgroundColor": COLORS["bg_dark"],
                    "padding": "2px 6px",
                    "borderRadius": "3px",
                    "fontSize": "10px",
                    "color": COLORS["accent_green"]
                })
            ])
        else:
            error_msg = response.json().get('detail', response.text)
            return html.Span(f"❌ Error ({response.status_code}): {error_msg}", style={"color": COLORS["accent_red"]})
    except requests.exceptions.ConnectionError:
        return html.Span(f"❌ Cannot connect to API at {API_BASE_URL}. Is it running?", style={"color": COLORS["accent_red"]})
    except Exception as e:
        return html.Span(f"❌ Error: {str(e)}", style={"color": COLORS["accent_red"]})


# Ingestion control handlers
@callback(
    Output('ingestion-status', 'children'),
    Input('start-ingestion-btn', 'n_clicks'),
    Input('stop-ingestion-btn', 'n_clicks'),
    State('expiry-date-input', 'value'),
    State('strike-price-input', 'value'),
    prevent_initial_call=True
)
def control_ingestion(start_clicks, stop_clicks, expiry_date, strike_price):
    ctx = dash.callback_context
    if not ctx.triggered:
        raise PreventUpdate

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'start-ingestion-btn':
        if not expiry_date or not strike_price:
            return html.Span("❌ Please enter Expiry Date and Strike Price", style={"color": COLORS["accent_red"]})

        try:
            # Call API endpoint to start ingestion
            print(f"Calling API: {API_BASE_URL}/start-ingestion")
            response = requests.post(f"{API_BASE_URL}/start-ingestion", json={
                "expiry_date": expiry_date,
                "strike_price": int(strike_price)
            }, timeout=10)

            print(f"Response status: {response.status_code}")
            if response.status_code == 200:
                return html.Span("✅ Ingestion started!", style={"color": COLORS["accent_green"]})
            else:
                error_msg = response.json().get('detail', response.text)
                return html.Span(f"❌ Error ({response.status_code}): {error_msg}", style={"color": COLORS["accent_red"]})
        except requests.exceptions.ConnectionError:
            return html.Span(f"❌ Cannot connect to API at {API_BASE_URL}. Is it running?", style={"color": COLORS["accent_red"]})
        except Exception as e:
            return html.Span(f"❌ Error: {str(e)}", style={"color": COLORS["accent_red"]})

    elif button_id == 'stop-ingestion-btn':
        try:
            # Call API endpoint to stop ingestion
            print(f"Calling API: {API_BASE_URL}/stop-ingestion")
            response = requests.post(f"{API_BASE_URL}/stop-ingestion", timeout=10)

            print(f"Response status: {response.status_code}")
            if response.status_code == 200:
                return html.Span("⏹️ Ingestion stopped", style={"color": COLORS["accent_yellow"]})
            else:
                error_msg = response.json().get('detail', response.text)
                return html.Span(f"❌ Error ({response.status_code}): {error_msg}", style={"color": COLORS["accent_red"]})
        except requests.exceptions.ConnectionError:
            return html.Span(f"❌ Cannot connect to API at {API_BASE_URL}. Is it running?", style={"color": COLORS["accent_red"]})
        except Exception as e:
            return html.Span(f"❌ Error: {str(e)}", style={"color": COLORS["accent_red"]})


if __name__ == "__main__":
    print("=" * 60)
    print("🐋 WHALE HUNTER DASHBOARD")
    print("=" * 60)
    print(f"\n📊 Dashboard: http://localhost:8050")
    print(f"🔌 WebSocket: {WS_URL}")
    print(f"\n🚀 NO POLLING - Pure WebSocket Push!")
    print(f"\n⚠️  Start API first:")
    print(f"   uv run uvicorn app.api.main:app --port 8001")
    print("=" * 60)

    app.run(debug=True, host="0.0.0.0", port=8050)
