import requests
import s3fs
import zarr
import boto3
from boto3.session import Session
from cmr import CollectionQuery
from requests.auth import HTTPBasicAuth

from urllib.parse import urlparse
from pystac import STAC_IO, Catalog


class HarmonyJob():

    def __init__(self, session, aws_keys, job_req):
        self.base_uri = 'https://harmony.earthdata.nasa.gov/jobs'
        self.job_req = job_req
        self.job_id = job_req['jobID']
        self._session = session
        self._aws_keys = aws_keys
        self.zarr_fs = s3fs.S3FileSystem(
            key=self._aws_keys['AccessKeyId'],
            secret=self._aws_keys['SecretAccessKey'],
            token=self._aws_keys['SessionToken'],
            client_kwargs={'region_name':'us-west-2'},
        )
        self._aws_session = Session(aws_access_key_id=aws_keys['AccessKeyId'],
                                    aws_secret_access_key=aws_keys['SecretAccessKey'],
                                    aws_session_token=aws_keys['SessionToken'])
        self.s3_client = self._aws_session.client('s3')
        STAC_IO.read_text_method = self._stac_auth_read

    def _stac_auth_read(self, uri):
        parsed = urlparse(uri)
        if parsed.scheme == 's3':
            bucket = parsed.netloc
            key = parsed.path[1:]
            obj = self.s3_client.Object(bucket, key)
            return obj.get()['Body'].read().decode('utf-8')
        elif parsed.scheme.startswith('http'):
            return self._session.get(uri).text
        else:
            return STAC_IO.default_read_text_method(uri)

    def status(self):
        req = self._session.get(f'{self.base_uri}/{self.job_id}').json()
        return req

    def stac_catalog(self):
        base_uri = 'https://harmony.earthdata.nasa.gov/stac'
        stac_catalog_uri = f'{base_uri}/{self.job_id}/'
        cat = Catalog.from_file(stac_catalog_uri)
        return cat

    def stac_uri(self):
        base_uri = 'https://harmony.earthdata.nasa.gov/stac'
        stac_catalog_uri = f'{base_uri}/{self.job_id}/'
        return self._session.get(stac_catalog_uri).json()

    def s3_file_uris(self):
        status = self.status()
        self.s3_files = [link['href'] for link in status['links'] if link['rel'] == 'data']
        return self.s3_files

    def s3_download_file(self, uri):
        parsed = urlparse(uri)
        file_name = parsed.path.split('/')[-1]
        if parsed.scheme == 's3':
            bucket = parsed.netloc
            key = parsed.path[1:]
            self.s3_client.download_file(bucket, key, f'data/{file_name}')
        else:
            print('not a s3 location')
            return None

    def s3_open_zarr(self, uri):
        zarr_store = self.zarr_fs.get_mapper(root=uri, check=False)
        zarr_dataset = zarr.open(zarr_store)
        return zarr_dataset

    def s3_open_netcdf(self, uri):
        return None

    def download(self):
        return None


class Harmony():
    """
    Helper class to send authenticated requests to Harmony
    params:
    - credentials: dictionary with NASA's Earthdata user and password keys
    """

    def __init__(self, credentials):
        base_auth_uri = 'https://urs.earthdata.nasa.gov/oauth/authorize'
        redirect_uri = 'https%3A%2F%2Fharmony.earthdata.nasa.gov%2Foauth2%2Fredirect'
        client_id = 'WocHIn_ABQ9FFxjAKTF5LQ'
        auth_url = f'{base_auth_uri}?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}'
        self._session = requests.session()
        credentials = HTTPBasicAuth(credentials['user'], credentials['password'])
        response = self._session.get(auth_url,
                                     auth=credentials,
                                     timeout=10,
                                     allow_redirects=True)
        if not response.ok:
            print('Authentication failed')
            return None
        else:
            aws_temp_keys = 'https://harmony.earthdata.nasa.gov/cloud-access'
            self.aws_keys = self._session.get(aws_temp_keys).json()

    def collection(self, id):
        base_uri = 'https://harmony.earthdata.nasa.gov'
        collection_uri = f'{base_uri}/{id}/ogc-api-coverages/1.0.0/collections?limit=100&f=json'
        collection = self._session.get(collection_uri)
        return collection.json()

    def subset_uri(self, params):
        base_uri = 'https://harmony.earthdata.nasa.gov'
        subset_uri = f"{base_uri}/{params['collection_id']}/ogc-api-coverages/" + \
                     f"{params['ogc-api-coverages_version']}/collections/{params['variable']}" + \
                     f"/coverage/rangeset?format={params['format']}&subset=time(\"{params['start']}\":\"{params['stop']}\")" + \
                     f"&subset=lat{params['lat']}&subset=lon{params['lon']}"
        return subset_uri

    def subset(self, params):
        base_uri = 'https://harmony.earthdata.nasa.gov'
        subset_uri = f"{base_uri}/{params['collection_id']}/ogc-api-coverages/" + \
                     f"{params['ogc-api-coverages_version']}/collections/{params['variable']}" + \
                     f"/coverage/rangeset?format={params['format']}&subset=time(\"{params['start']}\":\"{params['stop']}\")" + \
                     f"&subset=lat{params['lat']}&subset=lon{params['lon']}"
        req = self._session.get(subset_uri).json()
        print(req)
        return HarmonyJob(self._session, self.aws_keys, req)
