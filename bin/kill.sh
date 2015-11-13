ps -aux |grep "JDBCRunner-1.0.0"|awk '{print $2}'|xargs kill -9 
