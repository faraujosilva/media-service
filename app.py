"""
Media Service - Aplicação de Exemplo
Demonstra como usar STORAGE_ENDPOINT de forma agnóstica de cloud provider
"""
import os
import logging
from flask import Flask, request, jsonify

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ============================================================================
# STORAGE CONFIGURATION
# Variáveis injetadas automaticamente pelo HCP Terraform Operator
# ============================================================================

STORAGE_ENDPOINT = os.getenv('STORAGE_ENDPOINT')
STORAGE_NAME = os.getenv('STORAGE_NAME')
STORAGE_REGION = os.getenv('STORAGE_REGION')  # AWS only
STORAGE_ACCESS_KEY = os.getenv('STORAGE_ACCESS_KEY')  # Azure only

# Detecta cloud provider automaticamente
IS_AWS = STORAGE_ENDPOINT and 's3' in STORAGE_ENDPOINT
IS_AZURE = STORAGE_ENDPOINT and 'blob' in STORAGE_ENDPOINT

logger.info(f"Storage Endpoint: {STORAGE_ENDPOINT}")
logger.info(f"Storage Name: {STORAGE_NAME}")
logger.info(f"Cloud Provider: {'AWS' if IS_AWS else 'Azure' if IS_AZURE else 'Unknown'}")


# ============================================================================
# STORAGE CLIENT - Abstração agnóstica de cloud
# ============================================================================

class StorageClient:
    """Cliente abstrato para storage - funciona com AWS S3 ou Azure Blob"""
    
    def __init__(self):
        if IS_AWS:
            import boto3
            self.client = boto3.client('s3', region_name=STORAGE_REGION)
            self.bucket = STORAGE_NAME
            self.provider = "AWS S3"
        
        elif IS_AZURE:
            from azure.storage.blob import BlobServiceClient
            connection_string = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={STORAGE_NAME};"
                f"AccountKey={STORAGE_ACCESS_KEY};"
                f"EndpointSuffix=core.windows.net"
            )
            self.client = BlobServiceClient.from_connection_string(connection_string)
            self.container_name = "media-files"
            self._ensure_container()
            self.provider = "Azure Blob Storage"
        
        else:
            raise ValueError("No storage configured! Check STORAGE_ENDPOINT env var")
    
    def _ensure_container(self):
        """Cria container Azure se não existir"""
        if IS_AZURE:
            try:
                self.client.create_container(self.container_name)
                logger.info(f"Created container: {self.container_name}")
            except Exception as e:
                logger.debug(f"Container already exists: {e}")
    
    def upload_file(self, file_name, content):
        """Upload file - funciona para AWS e Azure"""
        if IS_AWS:
            self.client.put_object(
                Bucket=self.bucket,
                Key=file_name,
                Body=content
            )
            logger.info(f"Uploaded to S3: {file_name}")
            return f"s3://{self.bucket}/{file_name}"
        
        elif IS_AZURE:
            blob_client = self.client.get_blob_client(
                container=self.container_name,
                blob=file_name
            )
            blob_client.upload_blob(content, overwrite=True)
            logger.info(f"Uploaded to Azure: {file_name}")
            return f"https://{STORAGE_NAME}.blob.core.windows.net/{self.container_name}/{file_name}"
    
    def download_file(self, file_name):
        """Download file - funciona para AWS e Azure"""
        if IS_AWS:
            response = self.client.get_object(Bucket=self.bucket, Key=file_name)
            content = response['Body'].read()
            logger.info(f"Downloaded from S3: {file_name}")
            return content
        
        elif IS_AZURE:
            blob_client = self.client.get_blob_client(
                container=self.container_name,
                blob=file_name
            )
            content = blob_client.download_blob().readall()
            logger.info(f"Downloaded from Azure: {file_name}")
            return content
    
    def list_files(self):
        """Lista arquivos - funciona para AWS e Azure"""
        if IS_AWS:
            response = self.client.list_objects_v2(Bucket=self.bucket)
            files = [obj['Key'] for obj in response.get('Contents', [])]
            logger.info(f"Listed {len(files)} files from S3")
            return files
        
        elif IS_AZURE:
            container_client = self.client.get_container_client(self.container_name)
            files = [blob.name for blob in container_client.list_blobs()]
            logger.info(f"Listed {len(files)} files from Azure")
            return files


# Inicializa cliente de storage
try:
    storage = StorageClient()
except Exception as e:
    logger.error(f"Failed to initialize storage: {e}")
    storage = None


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/health')
def health():
    """Health check"""
    return jsonify({
        "status": "healthy",
        "storage": {
            "configured": storage is not None,
            "provider": storage.provider if storage else None,
            "endpoint": STORAGE_ENDPOINT
        }
    })


@app.route('/ready')
def ready():
    """Readiness check"""
    if not storage:
        return jsonify({"status": "not ready", "reason": "storage not configured"}), 503
    
    return jsonify({"status": "ready"})


@app.route('/upload', methods=['POST'])
def upload():
    """Upload arquivo para storage"""
    if not storage:
        return jsonify({"error": "Storage not configured"}), 503
    
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Empty filename"}), 400
    
    try:
        content = file.read()
        url = storage.upload_file(file.filename, content)
        
        return jsonify({
            "success": True,
            "filename": file.filename,
            "url": url,
            "provider": storage.provider
        })
    
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/files')
def list_files():
    """Lista arquivos no storage"""
    if not storage:
        return jsonify({"error": "Storage not configured"}), 503
    
    try:
        files = storage.list_files()
        return jsonify({
            "files": files,
            "count": len(files),
            "provider": storage.provider
        })
    
    except Exception as e:
        logger.error(f"List failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/download/<filename>')
def download(filename):
    """Download arquivo do storage"""
    if not storage:
        return jsonify({"error": "Storage not configured"}), 503
    
    try:
        content = storage.download_file(filename)
        return content, 200, {
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': f'attachment; filename={filename}'
        }
    
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/info')
def info():
    """Informações sobre a configuração"""
    return jsonify({
        "app": "media-service",
        "version": os.getenv('APP_VERSION', '1.0.0'),
        "environment": os.getenv('APP_ENV', 'development'),
        "storage": {
            "provider": storage.provider if storage else None,
            "endpoint": STORAGE_ENDPOINT,
            "name": STORAGE_NAME,
            "region": STORAGE_REGION if IS_AWS else None
        },
        "platform": {
            "provisioned_by": "hcp-terraform-operator",
            "managed_by": "argocd",
            "abstraction_level": "flavor-based"
        }
    })


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
