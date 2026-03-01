import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

from facebook_business.api import FacebookAdsApi
from facebook_business.session import FacebookSession
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.exceptions import FacebookRequestError

def extract_ad_creative(
    access_token: str,
    account_id: str,
    ad_ids: list[str],
) -> pd.DataFrame:
    """
    Extract Facebook Ads ad creative
    ---
    Principles:
        1. Initialize Facebook Ads client
        2. Loop each ad_id
        3. Make API call for Ad(ad_id) endpoint
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---
    Returns:    
        1. DataFrame:
            Flattened ad creative records
    """

    # Validate input
    if not ad_ids:
        print(
            "⚠️ [EXTRACT] No input ad_ids for Facebook Ads account_id "
            f"{account_id} then extraction will be suspended."
        )

        return pd.DataFrame()

    # Initialize Facebook Ads SDK client
    try:
        print(
            "🔍 [EXTRACT] Initializing Facebook Ads client with account_id "
            f"{account_id}..."
        )

        ad_creative_session = FacebookSession(
            access_token=access_token,
            timeout=180,
        )

        ad_creative_api = FacebookAdsApi(ad_creative_session)

        print(
            "✅ [EXTRACT] Successfully initialized Facebook Ads client for account_id "
            f"{account_id}."
        )

    except Exception as e:
        error = RuntimeError(
            "❌ [EXTRACT] Failed to initialize Facebook Ads client for account_id "
            f"{account_id} due to "
            f"{e}."
        )
        error.retryable = False
        raise error from e

    # Make Facebook Ads API call for ad creative
    rows: list[dict] = []
    
    print(
        "🔍 [EXTRACT] Extracting Facebook Ads ad creative for account_id "
        f"{account_id} with "
        f"{len(ad_ids)} ad_id(s)..."
    )

    for ad_id in ad_ids:
        try:
            ad = Ad(ad_id, api=ad_creative_api).api_get(fields=["creative"])
            creative_id = ad.get("creative", {}).get("id")

            if not creative_id:
                rows.append(
                    {
                        "account_id": account_id,
                        "ad_id": ad_id,
                        "creative_id": None,
                        "thumbnail_url": None,
                    }
                )
                continue

            creative = AdCreative(
                creative_id,
                api=ad_creative_api,
            ).api_get(fields=["thumbnail_url"])

            rows.append(
                {
                    "account_id": account_id,
                    "ad_id": ad_id,
                    "creative_id": creative_id,
                    "thumbnail_url": creative.get("thumbnail_url"),
                }
            )

        except FacebookRequestError as e:
            api_error_code = None
            http_status = None

            try:
                api_error_code = e.api_error_code()
                http_status = e.http_status()
            except Exception:
                pass

        # Expired token
            if api_error_code == 190:
                error = RuntimeError(
                    "❌ [EXTRACT] Failed to extract Facebook Ads ad creative for account_id "
                    f"{account_id} due to expired or invalid access token."
                )
                error.retryable = False
                raise error from e

        # Retryable API error
            if (
                (http_status and http_status >= 500)
                or api_error_code in {
                    1, 
                    2, 
                    4, 
                    17, 
                    80000
                }
            ):
                error = RuntimeError(
                    "⚠️ [EXTRACT] Failed to extract Facebook Ads ad creative for ad_id "
                    f"{ad_id} due to API error "
                    f"{e} then this request is eligible to retry."
                )
                error.retryable = True
                raise error from e

        # Non-retryable API error
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads ad creative for ad_id "
                f"{ad_id} due to API error "
                f"{e} then this request is not eligible to retry."
            )
            error.retryable = False
            raise error from e

        # Unknown non-retryable error
        except Exception as e:
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract Facebook Ads creative for ad_id "
                f"{ad_id} due to "
                f"{e}."
            )
            error.retryable = False
            raise error from e

    df = pd.DataFrame(rows)

    print(
        "✅ [EXTRACT] Successfully extracted "
        f"{len(df)}/{len(ad_ids)} row(s) of Facebook Ads ad creative."
    )

    return df