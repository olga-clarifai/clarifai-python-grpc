import sys

def show_progress_bar(i, total):
    done = int(100 * i / total)
    sys.stdout.write("\r[%s%s] %d%s complete " % ('=' * done, ' ' * (100-done), done, '%'))    
    sys.stdout.flush()
    if i == total:
        sys.stdout.write("\n")    
        sys.stdout.flush()