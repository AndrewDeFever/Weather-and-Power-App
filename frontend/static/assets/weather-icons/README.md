# Static icon assets

Static SVG files imported unmodified from upstream. They are not referenced by application code yet. Served by the existing `/static/` route (for example `/static/icons/weather/clear-day.svg`).

| Directory | Source | License file |
| --- | --- | --- |
| `weather/` | Meteocons (Bas Milius) | [weather/LICENSE](weather/LICENSE) (MIT) |
| `ui/` | Lucide | [ui/LICENSE](ui/LICENSE) (ISC, plus MIT for Feather-derived icons) |

Files were copied byte-for-byte; filenames are the upstream names. Do not restyle them in place.

## weather/ - Meteocons

- Project: https://github.com/basmilius/meteocons (site: https://meteocons.com)
- Package: `@meteocons/svg-static` **0.1.0** ("Static SVG weather icons without SMIL animations"), upstream package source directory `packages/svg-static` at gitHead `1d821149b94a08f23c85e5042e8a61a3fcd82cf7`
- Obtained from: https://registry.npmjs.org/@meteocons/svg-static/-/svg-static-0.1.0.tgz (SHA-1 `8edf2246ef51b9be5994e446b920d32b35c1878d`, matches the registry value). The static SVGs are build output and are not committed to the GitHub repository, so the published package is the official source.
- Style: `fill` (path inside the package: `fill/<name>.svg`). Repository `main` at time of import: `70dfb1d6e30dc9e791cfb0e4c5b5e5e60e972aa0`.
- License: MIT, Copyright (c) 2020-present Bas Milius. Notice retained in `weather/LICENSE`.

Notes: files have a `viewBox` of `0 0 128 128` but no `width`/`height`, so size them via the consumer. Internal element ids (for example gradient ids) repeat across files; use via `<img>`/CSS background, not inline, to avoid id collisions. Meteocons v3 has no dedicated "showers" icon; `partly-cloudy-day-rain` is imported as the nearest actual file.

| File | Intended general use |
| --- | --- |
| `clear-day.svg` | Clear / sunny, daytime |
| `clear-night.svg` | Clear, nighttime |
| `partly-cloudy-day.svg` | Partly cloudy / mostly sunny, daytime |
| `partly-cloudy-night.svg` | Partly cloudy / mostly clear, nighttime |
| `cloudy.svg` | Cloudy |
| `overcast.svg` | Overcast / mostly cloudy |
| `rain.svg` | Rain |
| `partly-cloudy-day-rain.svg` | Rain showers (intermittent rain with clouds and sun) |
| `thunderstorms.svg` | Thunderstorms |
| `snow.svg` | Snow |
| `fog.svg` | Fog / mist |
| `wind.svg` | Wind / breezy |

## ui/ - Lucide

- Project: https://github.com/lucide-icons/lucide (site: https://lucide.dev/icons/)
- Release: **1.58.0**, tag `1.58.0`, commit `7b0196e249ca09ca27c584c497aca9a8cab8a7f5`
- Upstream path: `icons/<name>.svg` at that tag, e.g. https://github.com/lucide-icons/lucide/blob/1.58.0/icons/map-pin.svg
- License: ISC, Copyright (c) 2026 Lucide Icons and Contributors. `ui/LICENSE` is the upstream file and also contains the MIT notice (Copyright (c) 2013-present Cole Bemis) that applies to icons derived from Feather. Of the files below, `clock.svg`, `external-link.svg` and `triangle-alert.svg` (formerly `alert-triangle`) appear in that Feather-derived list.

Notes: 24x24 outline icons using `stroke="currentColor"`, so they inherit text color.

| File | Intended general use |
| --- | --- |
| `map-pin.svg` | Site location |
| `clock.svg` | Time / observation or valid time |
| `wind.svg` | Wind |
| `droplets.svg` | Humidity / precipitation |
| `triangle-alert.svg` | Warnings / alerts |
| `external-link.svg` | Link that opens externally (e.g. NWS pages) |
| `thermometer.svg` | Temperature |
