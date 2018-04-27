import logging
import sys
import paramiko
from reportloader.utils.config import Config
from reportloader.utils.logger import StreamLogger


class SSH_Client():
    config_file = '/etc/opt/tw/reporter/reporter.conf'    
    logging_file = '/etc/opt/tw/reporter/logging.conf'
        
    def __init__(self):
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.host = Config.getParam('agora', 'ssh_host')
        self.user = Config.getParam('agora', 'ssh_user')
        self.password = Config.getParam('agora', 'ssh_pass')
        self.db_host = Config.getParam('agora', 'db_host')
        self.db_user = Config.getParam('agora', 'db_user')
        self.db_password = Config.getParam('agora', 'db_pass')
        self.db_name = Config.getParam('agora', 'db_name')
                
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # Update live agora server
        self.client.connect(
                hostname = self.host,
                port = 22,
                username = self.user,
                password = self.password,
                )
        self.mysql_prefix = 'mysql -u %s -p%s %s ' % (self.db_user, 
                                                      self.db_password, 
                                                      self.db_name)
        # Make temporary directory
        self.tmp_dir = self.do_cmd('mktemp -d').strip()
                                        
    def do_cmd(self,cmd):
        channel = self.client.get_transport().open_session()
        channel.settimeout(None)
        channel.exec_command(cmd)
        exit_status = channel.recv_exit_status()
        stdout = channel.recv(1024).decode()
        stderr = channel.recv_stderr(1024).decode()
        channel.close()

        if exit_status != 0:
            self.stream_logger.error('Error pushing to project agora ui')
            self.stream_logger.error('Failed command "%s". Exit status "%s". Stderr "%s"' % (cmd, exit_status, stderr))
            
        return stdout
    
    def get_temp_dir(self):
        return self.do_cmd('mktemp -d').strip()
    
    def get_file(self, remote, local):
        sftp_client = self.client.open_sftp()
        # Dump to local file
        sftp_client.get(remote, local)
        sftp_client.close()

    def put_file(self, local, remote):
        sftp_client = self.client.open_sftp()
        # Dump to local file
        sftp_client.put(local, remote)
        sftp_client.close()
        