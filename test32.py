i = 630

with open(r'previous_run\main_loop_node.bin', 'wb') as file:
                    file.write(i.to_bytes(32, byteorder='big'))