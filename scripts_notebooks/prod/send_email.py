import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import logging
import sys

# Configure logging first (before loading env vars so we can log issues)
script_dir = os.path.dirname(os.path.abspath(__file__))
# Go up two levels: scripts_notebooks/prod -> scripts_notebooks -> project_root
project_root = os.path.dirname(os.path.dirname(script_dir))
log_file_path = os.path.join(project_root, 'logs', 'email_sending.log')

# Ensure logs directory exists
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from project root
env_path = os.path.join(project_root, '.env')
logger.info(f"Loading environment variables from: {env_path}")
load_dotenv(dotenv_path=env_path)

# Email configuration from environment variables
GMAIL_EMAIL = os.getenv("GMAIL_EMAIL")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")

logger.info(f"Environment variables loaded - GMAIL_EMAIL: {'SET' if GMAIL_EMAIL else 'NOT SET'}, RECIPIENT_EMAIL: {'SET' if RECIPIENT_EMAIL else 'NOT SET'}")


def validate_environment():
    """Validate that all required environment variables are set."""
    required_vars = [
        "GMAIL_EMAIL",
        "GMAIL_APP_PASSWORD", 
        "RECIPIENT_EMAIL"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        return False
    
    logger.info("All required environment variables are set")
    return True


def send_simple_email(recipient_email: str, subject: str, body: str = ''):
    """
    Send an email using Gmail SMTP.
    
    Parameters
    ----------
    recipient_email : str
        Email address of the recipient.
    subject : str
        Subject line of the email.
    body : str, optional
        Email body content. If empty, sends email with just subject.
    """
    try:
        # Create message
        msg = MIMEText(body)
        msg['From'] = GMAIL_EMAIL
        msg['To'] = recipient_email
        msg['Subject'] = subject
        
        # Create SMTP session
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()  # Enable security
        server.login(GMAIL_EMAIL, GMAIL_APP_PASSWORD)
        
        # Send email
        server.sendmail(GMAIL_EMAIL, recipient_email, msg.as_string())
        server.quit()
        
        logger.info(f"Email sent successfully to {recipient_email} with subject: {subject}")
        
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        raise


def main():
    """Main function to send pipeline status email."""
    logger.info("="*70)
    logger.info("Starting email notification script")
    logger.info(f"Script arguments: {sys.argv}")
    
    # Validate environment
    if not validate_environment():
        logger.error("Environment validation failed. Exiting.")
        sys.exit(1)
    
    # Get status from command line argument (0 = success, 1 = failure)
    # Optional third argument: error message
    if len(sys.argv) < 2:
        logger.error("Usage: python send_email.py <status> [error_message] where status is 0 (success) or 1 (failure)")
        sys.exit(1)
    
    try:
        status_code = int(sys.argv[1])
        logger.info(f"Status code received: {status_code}")
        
        # Get error message if provided (for failures)
        error_message = sys.argv[2] if len(sys.argv) > 2 else None
        
        if status_code == 0:
            subject = "Daily Supervision Report: Success"
            body = "The daily supervision pipeline completed successfully."
        elif status_code == 1:
            subject = "Daily Supervision Report: Failure"
            if error_message:
                # Format error message for email body
                body = f"""The daily supervision pipeline failed.

Error Details:
{error_message}

Please check the logs for more information."""
            else:
                body = "The daily supervision pipeline failed. Please check the logs for more information."
        else:
            logger.error(f"Invalid status code: {status_code}. Must be 0 or 1.")
            sys.exit(1)
        
        logger.info(f"Preparing to send email with subject: {subject}")
        logger.info(f"Recipient: {RECIPIENT_EMAIL}")
        logger.info(f"From: {GMAIL_EMAIL}")
        
        # Send email
        send_simple_email(
            recipient_email=RECIPIENT_EMAIL,
            subject=subject,
            body=body
        )
        
        logger.info("="*70)
        logger.info("Email notification script completed successfully")
        logger.info("="*70)
        
    except ValueError as e:
        logger.error(f"Invalid status argument: {sys.argv[1]}. Must be 0 or 1. Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error("="*70)
        logger.error(f"Error in main process: {e}")
        import traceback
        logger.error(traceback.format_exc())
        logger.error("="*70)
        sys.exit(1)


if __name__ == "__main__":
    main()
