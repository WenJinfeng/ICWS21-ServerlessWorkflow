package com.code.orchestration;

public class logStreamEntity {
    private String streamid;
    private String streamtype;
    private String streamtime;
    private String streamnext;

    public logStreamEntity() {
        super();
    }

    public logStreamEntity(String streamid, String streamtype, String streamtime, String streamnext) {
        this.streamid = streamid;
        this.streamtype = streamtype;
        this.streamtime = streamtime;
        this.streamnext = streamnext;
    }

    public String getStreamid() {
        return streamid;
    }

    public void setStreamid(String streamid) {
        this.streamid = streamid;
    }

    public String getStreamtype() {
        return streamtype;
    }

    public void setStreamtype(String streamtype) {
        this.streamtype = streamtype;
    }

    public String getStreamtime() {
        return streamtime;
    }

    public void setStreamtime(String streamtime) {
        this.streamtime = streamtime;
    }

    public String getStreamnext() {
        return streamnext;
    }

    public void setStreamnext(String streamnext) {
        this.streamnext = streamnext;
    }
}
