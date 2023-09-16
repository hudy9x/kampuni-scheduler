package cronjob

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

func ReportDaily() {
	c := cron.New()

	job := func() {
		fmt.Println("Công việc buổi sáng đã được thực hiện vào thời gian:", time.Now())
	}

	job2 := func() {
		fmt.Println("Công việc buổi sáng đã được thực hiện vào thời gian:", time.Now())
	}

	_, err := c.AddFunc("* * * * *", job2)
	_, err2 := c.AddFunc("* * * * *", job)

	if err != nil {
		fmt.Println("Lỗi khi thêm công việc buổi sáng vào lịch:", err)
		return
	}
	if err2 != nil {
		fmt.Println("Lỗi khi thêm công việc buổi sáng vào lịch:", err)
		return
	}

	c.Start()

	select {}
}
