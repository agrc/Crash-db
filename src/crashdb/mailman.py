from smtplib import SMTP
from email.mime.text import MIMEText


class MailMan:
    def __init__(self, recipients, testing=False):
        """
        split multiple emails recipients with a ';' (e.g. recipients='hello@test.com;hello2@test.com')
        """
        self.sender = 'noreply@utah.gov'
        self.server = 'send.state.ut.us'
        self.port = 25
        self.testing = testing

        if recipients is None:
            raise Exception('You must provide recipients')

        self.recipients = recipients

    def deliver(self, subject, body, recipients=False):
        """
        sends an email using the agrcpythonemailer@gmail.com account
        """

        if not recipients:
            recipients = self.recipients

        recipients = recipients.split(';')

        message = MIMEText(body)
        message['Subject'] = subject
        message['From'] = self.sender
        message['To'] = ','.join(recipients)

        if self.testing:
            print('***Begin Test Email Message***')
            print(message)
            print('***End Test Email Message***')

            return

        s = SMTP(self.server, self.port)

        s.sendmail(self.sender, recipients, message.as_string())
        s.quit()

        print('email sent')
