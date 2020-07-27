# Any modules which add endpoints onto the app must be imported here
from . import gateway, s3

# Per ASGI conventions, main entry point is accessible as "exodus_gw:application"
from .app import app as application
