i = 660
change = "max"
if change == "main":
    with open(r'previous_run\main_loop_node.bin', 'wb') as file:
                        file.write(i.to_bytes(32, byteorder='big'))

if change == "max":
    with open(r'previous_run\max_update_node', 'wb') as file:
                        file.write(i.to_bytes(32, byteorder='big'))