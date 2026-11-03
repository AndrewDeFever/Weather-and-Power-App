# Vendored Frontend Dependencies

This directory contains browser dependencies intentionally committed to the repository rather than loaded from a runtime CDN.

## Leaflet

- Product: Leaflet
- Version: 1.9.4
- License: BSD-2-Clause
- Upstream: https://leafletjs.com/
- Runtime location: `frontend/static/vendor/leaflet/`
- Provenance/details: `frontend/static/vendor/leaflet/NOTICE.txt`
- License text: `frontend/static/vendor/leaflet/LICENSE`

### Integrity baseline

Portfolio CI verifies the Git blob IDs of the vendored runtime files below. Any change requires deliberate review and a baseline update.

- `leaflet.js`: `a3bf693d0fcaff885cc5f21b3a699aaa567b3533`
- `leaflet.css`: `5bffc1c0fcc3ab724643f94f978d1bd23244d690`
- `leaflet-assets.js`: `401b76c9e535928559245416c6662d4067827e0d`

### Update procedure

1. Review the desired upstream Leaflet release and its release/security notes.
2. Replace the vendored distribution files deliberately; preserve the BSD-2-Clause license and update `NOTICE.txt` with the new version/provenance.
3. Re-apply intentional local packaging changes, including embedded Leaflet image assets, and syntax-check the resulting JavaScript.
4. Update the versioned frontend references in `frontend/index.html`.
5. Recompute the Git blob IDs with `git hash-object <file>` and update this inventory and the CI integrity assertions.
6. Run Portfolio CI and manually verify map initialization, controls, tiles, overlays, and attribution before merge.

Do not silently replace vendored runtime files or automatically track a moving upstream tag.
