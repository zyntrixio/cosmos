from admin.app import create_app
from admin.config import admin_settings

app = create_app()

if __name__ == "__main__":
    app.run(debug=admin_settings.FLASK_DEBUG, port=admin_settings.FLASK_DEV_PORT)
