package net.helenus.core;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public interface Auditor {

    public default String getCurrentAuditor() { return "unknown"; }

    public default Date now() {
        Date in = new Date();
        LocalDateTime ldt = LocalDateTime.ofInstant(in.toInstant(), ZoneId.systemDefault());
        Date now = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
        return now;
    }

}
