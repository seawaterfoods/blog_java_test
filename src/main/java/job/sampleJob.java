package job;


import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class sampleJob
{

        @Scheduled(cron = "*/5 * * * * *")
    public void errorJob()
    {
        throw new OutOfMemoryError("test catch error.");
    }

}
