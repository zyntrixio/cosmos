from admin.app import create_app
from cosmos.core.config import settings

app = create_app()

if __name__ == "__main__":
    app.run(debug=settings.FLASK_DEBUG, port=settings.FLASK_DEV_PORT)
