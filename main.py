import os
import json
import logging
import base64
import copy

from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging

# Import shared code from your published package.
from timesheet_common_timesheet_mfdenison_hopkinsep.models import TimeLog
from timesheet_common_timesheet_mfdenison_hopkinsep.serializers import TimeLogSerializer
from timesheet_common_timesheet_mfdenison_hopkinsep.utils.dashboard import send_dashboard_update

# Initialize Cloud Logging; logs will appear in Google Cloud Logging.
client = cloud_logging.Client()
client.setup_logging()

# Set up the standard Python logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Pub/Sub publisher client.
publisher = pubsub_v1.PublisherClient()
project_id = os.environ.get("PROJECT_ID", "hopkinstimesheetproj")
pto_deduction_topic = f'projects/{project_id}/topics/pto_deduction_queue'
dashboard_topic = f'projects/{project_id}/topics/dashboard-queue'

def timelog_processing_handler(event, context):
    """
    Cloud Function triggered by a Pub/Sub message that processes a timelog submission.

    Steps performed:
     1. Decode the message (handles potential double JSON encoding).
     2. Patch the payload so that if "employee" exists, it is replaced by "employee_id".
     3. Validate the data via TimeLogSerializer.
     4. Create a new TimeLog record and save it.
     5. If PTO hours are provided, publish a PTO deduction message; otherwise, send a dashboard refresh update.

    If the function completes without error, the message is automatically acknowledged.
    """
    employee_id = None
    pto_hours = None
    try:
        # Decode the Pub/Sub message data (which is base64-encoded).
        raw_data = base64.b64decode(event["data"]).decode("utf-8")
        logger.info(f"Raw message received: {raw_data}")

        # Decode the JSON. If the decoded value is still a JSON string, decode it again.
        first_pass = json.loads(raw_data)
        data = json.loads(first_pass) if isinstance(first_pass, str) else first_pass

        # Make a deep copy so we can mutate it safely.
        sanitized_data = copy.deepcopy(data)

        # Patch field name: If "employee" is provided instead of "employee_id", rename the key.
        if "employee" in sanitized_data:
            sanitized_data["employee_id"] = sanitized_data.pop("employee")
            logger.info("Patched 'employee' key to 'employee_id'.")

        if not isinstance(sanitized_data, dict):
            raise ValueError("Invalid message format: expected a JSON object.")

        employee_id = sanitized_data.get("employee_id")
        logger.info(f"Received timelog submission for employee ID: {employee_id}")
        logger.info(f"Sanitized data for serialization: {json.dumps(sanitized_data)}")

        # Validate the sanitized data with your serializer.
        serializer = TimeLogSerializer(data=sanitized_data)
        serializer.is_valid(raise_exception=True)

        # Create the TimeLog record. (Assumes your model's manager provides a `create` method similar to Django.)
        timesheet = TimeLog.objects.create(**serializer.validated_data)
        timesheet.save()
        logger.info(f"TimeLog created successfully for employee {employee_id}")

        pto_hours = serializer.validated_data.get("pto_hours")
        if pto_hours:
            pto_message = {
                "employee_id": employee_id,
                "pto_hours": pto_hours
            }
            publisher.publish(
                pto_deduction_topic,
                data=json.dumps(pto_message).encode("utf-8")
            )
            logger.info(f"PTO deduction message published for employee {employee_id} with {pto_hours} hours.")
        else:
            # If no PTO hours are provided, send a dashboard refresh message.
            refresh_payload = send_dashboard_update(
                employee_id,
                "refresh_data",
                "Time log created, please refresh dashboard data."
            )
            publisher.publish(
                dashboard_topic,
                data=json.dumps(refresh_payload).encode("utf-8")
            )
            logger.info(f"Dashboard refresh message published for employee {employee_id}.")

    except Exception as e:
        logger.exception(f"Error creating time log for employee {employee_id or 'unknown'}: {str(e)}")
        # Raising the exception will signal a function failure to Cloud Functions (triggering a retry if configured).
        raise
