all:
	#This script will create a new tmux session named 'mqtt', start the
	#server code and detach the session to allow it to continue to run
	#after the remote session has ended.
	tmux new-session -d -s mqtt
	tmux send-keys 'python mqtt.py' C-m
	tmux detach -s mqtt
	#To reattach the session enter 'tmux attach -t mqtt'
	#To re-detach the session after attaching it enter
	#CTR-B then "d" while in tmux
