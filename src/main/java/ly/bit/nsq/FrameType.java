package ly.bit.nsq;

import ly.bit.nsq.exceptions.NSQException;

public enum FrameType {
	FRAMETYPERESPONSE, FRAMETYPEERROR, FRAMETYPEMESSAGE;
	
	public static FrameType fromInt(int typeId) throws NSQException {
		switch (typeId) {
		case 0:
			return FrameType.FRAMETYPERESPONSE;
		case 1:
			return FrameType.FRAMETYPEERROR;
		case 2:
			return FrameType.FRAMETYPEMESSAGE;
		default:
			throw new NSQException("Invalid Frame Type");
		}
	}
}
