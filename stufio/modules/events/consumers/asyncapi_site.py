from typing import TYPE_CHECKING
import logging

from faststream._compat import json_dumps
import logging


if TYPE_CHECKING:
    from faststream.asyncapi.schema import Schema

logger = logging.getLogger(__name__)

# Updated URLs to the latest versions
ASYNCAPI_JS_DEFAULT_URL = "https://unpkg.com/@asyncapi/react-component@1.0.0-next.47/browser/standalone/index.js"
ASYNCAPI_CSS_DEFAULT_URL = "https://unpkg.com/@asyncapi/react-component@1.0.0-next.46/styles/default.min.css"

def get_asyncapi_html(
    schema: "Schema",
    sidebar: bool = True,
    info: bool = True,
    servers: bool = True,
    operations: bool = True,
    messages: bool = True,
    schemas: bool = True,
    errors: bool = True,
    expand_message_examples: bool = True,
    title: str = "Stufio Events API",  # Customized title
    asyncapi_js_url: str = ASYNCAPI_JS_DEFAULT_URL,
    asyncapi_css_url: str = ASYNCAPI_CSS_DEFAULT_URL,
) -> str:
    """Generate HTML for displaying an AsyncAPI document."""
    schema_json = schema.to_json()

    # Custom config with logo and additional options
    config = {
        "schema": schema_json,
        "config": {
            "show": {
                "sidebar": sidebar,
                "info": info,
                "servers": servers,
                "operations": operations,
                "messages": messages,
                "schemas": schemas,
                "errors": errors,
            },
            "expand": {
                "messageExamples": expand_message_examples,
            },
            "sidebar": {
                "showServers": "byDefault",
                "showOperations": "byDefault",
            },
        },
    }

    return (
        """
    <!DOCTYPE html>
    <html>
        <head>
    """
        f"""
        <title>{title} AsyncAPI</title>
    """
        """
        <link rel="icon" href="https://www.asyncapi.com/favicon.ico">
        <link rel="icon" type="image/png" sizes="16x16" href="https://www.asyncapi.com/favicon-16x16.png">
        <link rel="icon" type="image/png" sizes="32x32" href="https://www.asyncapi.com/favicon-32x32.png">
        <link rel="icon" type="image/png" sizes="194x194" href="https://www.asyncapi.com/favicon-194x194.png">
    """
        f"""
        <link rel="stylesheet" href="{asyncapi_css_url}">
    """
        """
        </head>

        <style>
        html {
            font-family: ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji;
            line-height: 1.5;
        }
        /* Custom CSS to make sidebar wider */
        @media (min-width: 1024px) {
            .aui-root .container\:base .sidebar--content {
                width: 20rem;
            }
            .aui-root .container\:base .sidebar {
                width: 22rem;
            }
        }
        /* Any other custom styles you need */
        </style>

        <body>
        <div id="asyncapi"></div>
    """
        f"""
        <script src="{asyncapi_js_url}"></script>
        <script>
    """
        f"""
            AsyncApiStandalone.render({json_dumps(config).decode()}, document.getElementById('asyncapi'));
    """
        """
        </script>
        </body>
    </html>
    """
    )

