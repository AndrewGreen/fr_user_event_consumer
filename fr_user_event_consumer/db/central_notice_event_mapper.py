from fr_user_event_consumer.central_notice_event import CentralNoticeEvent

def new( json_string ):
    return CentralNoticeEvent( json_string )