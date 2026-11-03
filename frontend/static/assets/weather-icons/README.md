# Static icon assets

These SVG assets are source-controlled with the frontend and deployed through the existing S3/CloudFront static path.

Application paths:

- Meteocons weather icons: `/static/assets/weather-icons/weather/<icon>.svg`
- Lucide UI icons: `/static/assets/weather-icons/ui/<icon>.svg`

## Weather icons

The files under `weather/` are Meteocons assets used by the forecast/current-conditions UI. The application maps NWS condition text to this bounded local icon set in `frontend/static/app.js`.

## UI icons

The files under `ui/` are Lucide icons reserved for local UI use as the map/dashboard controls are refined.

The upstream license files are retained alongside each asset set. Do not remove them when adding or updating icons.
