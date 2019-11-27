if [ $# -eq 0 ]; then
    python3 producer.py
else
    if [ $1 = "monitor" ]; then
        python3 monitor.py
    else
        python3 producer.py
    fi
fi
