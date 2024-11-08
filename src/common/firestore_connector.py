from google.cloud import firestore
from google.cloud import secretmanager
import os

class FirestoreManager:
    def __init__(self, project_id, credentials_path=None, secret_id='service_acount_dalaka_v2'):
        self.project_id = project_id
        self.client = self.authenticate(credentials_path, secret_id)

    def authenticate(self, credentials_path, secret_id):
        try:
            # Check if credentials_path is provided and exists
            if credentials_path and os.path.exists(credentials_path):
                # Authentication using the local credentials file
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
                print(f"Using local credentials from: {credentials_path}")
            else:
                # Fallback to authentication using Secret Manager
                print("Fetching credentials from Secret Manager.")
                secret_client = secretmanager.SecretManagerServiceClient()
                secret_name = f"projects/datalake-v2-424516/secrets/{secret_id}/versions/latest"
                response = secret_client.access_secret_version(request={"name": secret_name})
                credentials_json = response.payload.data.decode("UTF-8")

                # Write credentials to a temporary file and set the environment variable
                with open("temp_credentials.json", "w") as cred_file:
                    cred_file.write(credentials_json)
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "temp_credentials.json"
                print("Using credentials from Secret Manager.")
        except Exception as e:
            raise RuntimeError(f"Error during authentication: {str(e)}")
        
        # Return the Firestore client for the specified project
        return firestore.Client(project=self.project_id)

    def get_collection_documents(self, collection_name):
        """
        Retrieves all documents from the specified collection.
        
        Parameters:
        collection_name (str): The name of the Firestore collection to retrieve documents from.
        
        Returns:
        list: A list of document data from the collection.
        """
        collection_ref = self.client.collection(collection_name)
        docs = collection_ref.stream()
        documents = [{doc.id: doc.to_dict()} for doc in docs]
        return documents

    def clean_cache(self, collection_name):
        """
        Deletes all documents in the specified collection.
        
        Parameters:
        collection_name (str): The name of the Firestore collection to be deleted.
        """
        collection_ref = self.client.collection(collection_name)
        docs = collection_ref.stream()

        batch = self.client.batch()
        count = 0
        for doc in docs:
            batch.delete(doc.reference)
            count += 1
            # Firestore has a limit of 500 operations per batch
            if count == 500:
                batch.commit()
                batch = self.client.batch()
                count = 0
        if count > 0:
            batch.commit()
        print(f"All documents in collection '{collection_name}' have been deleted.")
