"""Find the drive_type / drive_id / file_id values sharepoint-to-staging needs,
starting from the labels you see in SharePoint (site name, library, file name).

Examples:
    python "Get Drive & File ID.py" --site "IDE Data Warehouse" --file WWS_Dataset.xlsx
    python "Get Drive & File ID.py" --site https://<tenant>.sharepoint.com/sites/Foo --file report.xlsx
    python "Get Drive & File ID.py" --site "IDE Data Warehouse" --library Documents --path "Reports/2026" --file report.xlsx
    python "Get Drive & File ID.py" --user someone@alleghenycounty.us --file report.xlsx

Needs client_id and client_secret in the environment or in a .env file in this folder or a parent.
"""
import argparse
import os
import sys
import time
import urllib.parse
from pathlib import Path

import pandas as pd
import requests
import urllib3
from dotenv import load_dotenv
from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session

# Fill out to search without command-line arguments (arguments override these):
SITE = 'IDE Data Warehouse'  # Site display name or site URL from the browser
USER = None                  # OneDrive owner's email (use instead of SITE)
FILE_NAME = 'WWS_Dataset.xlsx'
LIBRARY = None               # Library name from the site's left nav, e.g. 'Documents'; None searches all
FOLDER_PATH = None           # Folder inside the library, e.g. 'Reports/2026'; exact lookup instead of search

GRAPH = 'https://graph.microsoft.com/v1.0'
DEFAULT_TENANT = 'e0273d12-e4cb-4eb1-9f70-8bba16fb968d'
TOP_N = 10


class LookupError_(Exception):
    """A lookup that should stop the script with a readable message."""


# ---------- Graph access ----------

def get_session(verify=True):
    # Nearest .env from this script's folder upward (works from any working directory)
    for folder in Path(__file__).resolve().parents:
        if (folder / '.env').is_file():
            load_dotenv(folder / '.env')
            break
    client_id = os.getenv('client_id')
    client_secret = os.getenv('client_secret')
    if not client_id or not client_secret:
        raise LookupError_('client_id and client_secret must be set (environment or a .env in this folder or a parent).')
    tenant = os.getenv('tenant_id', DEFAULT_TENANT)

    oauth = OAuth2Session(client=BackendApplicationClient(client_id=client_id))
    token = oauth.fetch_token(
        token_url='https://login.microsoftonline.com/{}/oauth2/v2.0/token'.format(tenant),
        scope='https://graph.microsoft.com/.default',
        auth=HTTPBasicAuth(client_id, client_secret),
        verify=verify)

    session = requests.Session()
    session.verify = verify
    session.headers['authorization'] = 'Bearer {}'.format(token['access_token'])
    return session


def graph_get(session, url, retries=3):
    """GET a Graph URL, retrying on throttling and raising a readable error."""
    if not url.startswith('http'):
        url = GRAPH + url
    for attempt in range(retries):
        r = session.get(url)
        if r.status_code in (429, 503) and attempt < retries - 1:
            time.sleep(int(r.headers.get('Retry-After', 5)))
            continue
        break
    if not r.ok:
        try:
            err = r.json().get('error', {})
            detail = '{}: {}'.format(err.get('code'), err.get('message'))
        except ValueError:
            detail = r.text[:200]
        raise requests.HTTPError('{} {} -> {}'.format(r.status_code, url.split('?')[0], detail), response=r)
    return r.json()


def graph_get_all(session, url):
    """GET a Graph collection, following @odata.nextLink pages."""
    items = []
    while url:
        page = graph_get(session, url)
        items.extend(page.get('value', []))
        url = page.get('@odata.nextLink')
    return items


# ---------- Pure helpers (no network) ----------

def parse_site_url(value):
    """Return (hostname, server-relative site path) for a SharePoint URL, or None if not a URL.

    'https://x.sharepoint.com/sites/Foo/Shared%20Documents/a.xlsx' -> ('x.sharepoint.com', '/sites/Foo')
    """
    parsed = urllib.parse.urlparse(value.strip())
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        return None
    parts = [urllib.parse.unquote(p) for p in parsed.path.split('/') if p]
    if len(parts) >= 2 and parts[0].lower() in ('sites', 'teams'):
        return parsed.netloc, '/{}/{}'.format(parts[0], parts[1])
    return parsed.netloc, ''


def pick_site(candidates, name):
    """Choose the site matching a display name; raise listing candidates when ambiguous."""
    wanted = name.strip().lower()
    exact = [s for s in candidates if (s.get('displayName') or '').strip().lower() == wanted]
    if not exact:
        exact = [s for s in candidates if (s.get('name') or '').strip().lower() == wanted]
    if len(exact) == 1:
        return exact[0]
    if not exact and len(candidates) == 1:
        return candidates[0]
    pool = exact or candidates
    if not pool:
        raise LookupError_('No SharePoint site found for "{}".'.format(name))
    lines = ['  {}. {}  {}'.format(i + 1, s.get('displayName'), s.get('webUrl')) for i, s in enumerate(pool)]
    raise LookupError_('"{}" matches {} sites. Re-run with --site set to one of these URLs:\n{}'.format(
        name, len(pool), '\n'.join(lines)))


def site_guid(site_id):
    """Graph site ids look like 'host,siteGuid,webGuid'; sharepoint-to-staging uses the site GUID."""
    parts = site_id.split(',')
    return parts[1] if len(parts) == 3 else site_id


def search_query(text):
    """Build the q='...' argument for drive search: double single quotes, then URL-encode."""
    return urllib.parse.quote("'{}'".format(text.replace("'", "''")), safe='')


def item_path_url(drive_id, folder_path, file_name):
    """URL for an exact path lookup inside a drive."""
    rel = '/'.join(p for p in (folder_path or '').replace('\\', '/').split('/') if p)
    rel = '{}/{}'.format(rel, file_name) if rel else file_name
    return '/drives/{}/root:/{}'.format(drive_id, urllib.parse.quote(rel, safe='/'))


def filter_drives(drives, library):
    """Keep the drive(s) whose name matches the library label; all drives if no label given."""
    if not library:
        return drives
    wanted = library.strip().lower()
    match = [d for d in drives if (d.get('name') or '').strip().lower() == wanted]
    if not match:
        names = ', '.join(sorted('"{}"'.format(d.get('name')) for d in drives))
        raise LookupError_('No library named "{}". Libraries on this site: {}'.format(library, names))
    return match


def rank_items(items, file_name):
    """Files only, deduplicated; exact name first, then name contains, then the rest; newest first in each."""
    wanted = file_name.strip().lower()
    seen, files = set(), []
    for it in items:
        if 'file' not in it or it.get('id') in seen:
            continue
        seen.add(it['id'])
        files.append(it)

    def tier(it):
        name = (it.get('name') or '').lower()
        return 0 if name == wanted else 1 if wanted in name else 2

    files.sort(key=lambda it: it.get('lastModifiedDateTime') or '', reverse=True)
    files.sort(key=tier)  # stable sort keeps newest-first within a tier
    return files


def folder_of(item):
    """'/drives/x/root:/Reports/2026' -> 'Reports/2026'; '/' for the library root; '?' when not reported.

    Search results usually omit the path; exact --path lookups include it.
    """
    path = (item.get('parentReference') or {}).get('path') or ''
    if 'root:' not in path:
        return '?'
    return urllib.parse.unquote(path.split('root:', 1)[1].lstrip('/')) or '/'


def deployment_values(item, site_id, default_drive_id):
    """Environment values for sharepoint-to-staging.

    The image's 'sites' mode only reads the site's default library, so files elsewhere
    (other libraries, OneDrive) use 'drives' mode with the item's own drive id.
    """
    drive_id = (item.get('parentReference') or {}).get('driveId')
    if site_id and drive_id == default_drive_id:
        return {'drive_type': 'sites', 'drive_id': site_guid(site_id), 'file_id': item['id']}
    return {'drive_type': 'drives', 'drive_id': drive_id, 'file_id': item['id']}


# ---------- Lookups ----------

def resolve_site(session, site):
    parsed = parse_site_url(site)
    if parsed:
        host, path = parsed
        return graph_get(session, '/sites/{}:{}'.format(host, path) if path else '/sites/{}'.format(host))
    candidates = graph_get_all(session, '/sites?search={}'.format(urllib.parse.quote(site)))
    return pick_site(candidates, site)


def find_items(session, drives, file_name, folder_path):
    items = []
    for d in drives:
        if folder_path is not None:
            try:
                items.append(graph_get(session, item_path_url(d['id'], folder_path, file_name)))
            except requests.HTTPError as e:
                if e.response is None or e.response.status_code != 404:
                    raise
            continue
        # Search tokenizes names; the stem catches files the full name misses.
        stem = os.path.splitext(file_name)[0]
        for q in dict.fromkeys([file_name, stem]):
            found = graph_get_all(session, "/drives/{}/root/search(q={})".format(d['id'], search_query(q)))
            items.extend(found)
            if any((f.get('name') or '').lower() == file_name.lower() for f in found):
                break
    return items


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    loc = p.add_mutually_exclusive_group()
    loc.add_argument('--site', help='SharePoint site display name or URL')
    loc.add_argument('--user', help="OneDrive owner's email / UPN")
    p.add_argument('--file', default=FILE_NAME, help='File name as shown in SharePoint')
    p.add_argument('--library', default=LIBRARY, help='Library name, e.g. "Documents" (default: all)')
    p.add_argument('--path', default=FOLDER_PATH, help='Folder inside the library for an exact lookup')
    p.add_argument('--all', action='store_true', help='List every file the search returned')
    p.add_argument('--insecure', action='store_true', help='Skip TLS certificate checks')
    args = p.parse_args(argv)
    if not args.site and not args.user:
        args.site, args.user = (SITE, None) if SITE else (None, USER)
    if not (args.site or args.user) or not args.file:
        p.error('need --site or --user, and --file')

    if args.insecure:
        urllib3.disable_warnings()
    session = get_session(verify=not args.insecure)

    site_id = None
    if args.user:
        default = graph_get(session, '/users/{}/drive'.format(urllib.parse.quote(args.user)))
        drives = [default]
        print('OneDrive: {}'.format(args.user))
    else:
        site = resolve_site(session, args.site)
        site_id = site['id']
        default = graph_get(session, '/sites/{}/drive'.format(site_id))
        drives = filter_drives(graph_get_all(session, '/sites/{}/drives'.format(site_id)), args.library)
        print('Site: {}  {}'.format(site.get('displayName'), site.get('webUrl')))
        print('Site id (sites mode drive_id): {}'.format(site_guid(site_id)))
        print('Libraries searched: {}'.format(', '.join(d.get('name') for d in drives)))

    ranked = rank_items(find_items(session, drives, args.file, args.path), args.file)
    if not ranked:
        hint = ('Check the folder path and file name.' if args.path is not None else
                'New or renamed files can take a while to appear in search; try --library and --path for an exact lookup.')
        raise LookupError_('No file matching "{}" found. {}'.format(args.file, hint))

    library_names = {d['id']: d.get('name') for d in drives}
    shown = ranked if args.all else ranked[:TOP_N]
    for i, it in enumerate(shown):
        ref = it.get('parentReference') or {}
        if 'path' not in ref and ref.get('driveId'):
            # Search results omit the folder; fetch it so same-named files can be told apart
            shown[i] = graph_get(session, '/drives/{}/items/{}'.format(ref['driveId'], it['id']))
    table = pd.DataFrame([{
        'name': it.get('name'),
        'library': library_names.get((it.get('parentReference') or {}).get('driveId'), ''),
        'folder': folder_of(it),
        'modified': (it.get('lastModifiedDateTime') or '')[:19],
        'webUrl': it.get('webUrl'),
    } for it in shown])
    table.index += 1
    with pd.option_context('display.max_colwidth', 120, 'display.width', 250):
        print('\nMatches (best first):')
        print(table.to_string())
    if len(ranked) > len(shown):
        print('... {} more; use --all to list them.'.format(len(ranked) - len(shown)))

    best = ranked[0]
    if best.get('name', '').lower() != args.file.lower():
        print('\nWARNING: no exact name match; #1 is the closest. Confirm it before using these values.')
    exact = [it for it in ranked if (it.get('name') or '').lower() == args.file.lower()]
    if len(exact) > 1:
        print('\nWARNING: {} files share this name; #1 is the most recently modified. '
              'Check the folder/webUrl columns or use --path.'.format(len(exact)))

    values = deployment_values(best, site_id, default['id'])
    print('\nValues for #1 ({}):'.format(best.get('name')))
    for k, v in values.items():
        print("    '{}': '{}',".format(k, v))
    if values['drive_type'] == 'drives' and site_id:
        print('Note: this file is not in the site\'s default library, so it needs drive_type "drives". '
              'sharepoint_upload.py only writes to the default library.')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (LookupError_, requests.HTTPError) as e:
        print('ERROR: {}'.format(e), file=sys.stderr)
        sys.exit(1)
