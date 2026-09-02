from mangum import Mangum
from app.api import app

handler = Mangum(app, api_gateway_base_path="/prod")