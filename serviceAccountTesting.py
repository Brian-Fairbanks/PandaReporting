from google.oauth2 import service_account
import googleapiclient.discovery

# Takes a TCESD2 Google Form ID, and uses a service account to reach out and grab all of the submitted form data
def getFormData(formID):
    SCOPES = ["https://www.googleapis.com/auth/forms.responses.readonly"]
    SERVICE_ACCOUNT_FILE = "sample-form-reader-395ab8f59999.json"

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    #     print(credentials)

    service = googleapiclient.discovery.build("forms", "v1", credentials=credentials)
    result = service.forms().responses().list(formId=formID).execute()

    return result


# First Testing form
def getSampleData():
    # The google form ID which you would like to read data on.
    form_id = "1YBFwQZ70FCa6S05jr-63FC9TfQs2Gf3kjwL5HT5GdQ0"
    return getFormData(form_id)


def main():
    formData = getSampleData()

    from Database import SQLDatabase

    db = SQLDatabase()
    db.insertForm(formData)


if __name__ == "__main__":
    main()
