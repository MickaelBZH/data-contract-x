"""ASGI entry point for `dcx api`.

Importing this module mounts the dcx routes onto the upstream
`datacontract.api:app`. `dcx api` starts uvicorn pointed at this module's `app`
attribute, so the running server exposes both upstream endpoints (lint, test,
export, …) and the dcx-mirrored endpoints (target, …).
"""

from datacontract.api import app  # re-exported

from dcx.api import (
    mirror_apply_snowflake_to_fastapi,
    mirror_enrich_all_to_fastapi,
    mirror_enrich_tags_to_fastapi,
    mirror_enrich_to_fastapi,
    mirror_export_to_fastapi,
    mirror_import_to_fastapi,
    mirror_snowflake_import_to_fastapi,
    mirror_target_to_fastapi,
    mount_info_endpoint,
)

mirror_target_to_fastapi(app)
mirror_import_to_fastapi(app)
mirror_snowflake_import_to_fastapi(app)
mirror_export_to_fastapi(app)
mirror_apply_snowflake_to_fastapi(app)
mirror_enrich_to_fastapi(app)
mirror_enrich_tags_to_fastapi(app)
mirror_enrich_all_to_fastapi(app)
mount_info_endpoint(app)

__all__ = ["app"]
