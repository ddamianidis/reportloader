from reportloader.reporterpuller import ReporterPuller
from reportloader.pushtoui.reporterpusher import ReporterPusher
from jrpc.server import JsonRPCServer 
from reportloader.pullmode import PullModeKeeper

def pull_data(platform, startdate, enddate):
    return ReporterPuller(platform, startdate, enddate).pull_data()

def push_data(platform, date, mode):
    data_pusher_cls = ReporterPusher(platform, date, mode)
    rs = data_pusher_cls.push_dailies_to_ui()
    return rs

def pull_mode_normal(platform):
    return PullModeKeeper.setModeAsNormal(platform)
    
def pull_mode_mute(platform):
    return PullModeKeeper.setModeAsMute(platform)
    
def main():
    
    jrpcserver = JsonRPCServer({
        'pull_data':pull_data,
        'push_data':push_data,
        'pull_mode_normal':pull_mode_normal,
        'pull_mode_mute':pull_mode_mute,
        
        }, 'tcp://*:5552')
    
    jrpcserver.run()
            
    return False        

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Encoutered unhandled exception %s' % e)
        raise
