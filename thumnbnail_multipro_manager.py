import time
import os
import logging
from urllib.parse import urlparse
from urllib.request import urlretrieve
from queue import Queue
from threading import Thread
import multiprocessing

import PIL
from PIL import Image

logging.basicConfig(filename='logfile.log', level=logging.DEBUG)


class ThumbnailMakerService(object):
    def __init__(self, home_dir='.'):
        self.home_dir = home_dir
        self.input_dir = self.home_dir + os.path.sep + 'incoming'
        self.output_dir = self.home_dir + os.path.sep + 'outgoing'
        self.img_queue = multiprocessing.JoinableQueue()
        # need the size of original files and of resized file
        self.dl_size = 0
        self.resized_size = multiprocessing.Value("i", 0)

    def download_image(self, dl_queue, dl_size_lock):
        while not dl_queue.empty():
            try:
                url = dl_queue.get(block=False)
                img_filename = urlparse(url).path.split('/')[-1]
                img_filepath = self.input_dir + os.path.sep + img_filename
                urlretrieve(url, img_filepath)
                # new
                with dl_size_lock:
                    self.dl_size += os.path.getsize(img_filepath)
                self.img_queue.put(img_filename)

                dl_queue.task_done()
            except Queue.Empty:
                logging.info("Queue empty")

    def download_images(self, img_url_list):
        if not img_url_list:
            return
        os.makedirs(self.input_dir, exist_ok=True)

        logging.info("beginning image downloads")

        start = time.perf_counter()
        for url in img_url_list:
            img_filename = urlparse(url).path.split('/')[-1]
            urlretrieve(url, self.input_dir + os.path.sep + img_filename)
            self.img_queue.put(img_filename)

        end = time.perf_counter()

        self.img_queue.put(None)

        logging.info("downloaded {} images in {} seconds".format(
            len(img_url_list), end - start))

    def perform_resizing(self):
        os.makedirs(self.output_dir, exist_ok=True)

        logging.info("beginning image resizing")

        target_sizes = [32, 64, 200]
        num_images = len(os.listdir(self.input_dir))

        start = time.perf_counter()
        while True:
            filename = self.img_queue.get()
            if filename:
                logging.info("resizing image {}".format(filename))
                orig_img = Image.open(self.input_dir + os.path.sep + filename)
                for basewidth in target_sizes:
                    img = orig_img
                    wpercent = (basewidth / float(img.size[0]))
                    hsize = int((float(img.size[1]) * float(wpercent)))
                    img = img.resize((basewidth, hsize), PIL.Image.LANCZOS)

                    new_filename = os.path.splitext(filename)[0] + \
                        '_' + str(basewidth) + os.path.splitext(filename)[1]
                    # new
                    out_filepath = self.output_dir + os.path.sep + new_filename
                    img.save(out_filepath)

                    # normally there's a internal lock for sync share value
                    # but sinvce we have two operations in one below:
                    with self.resized_size.get_lock():
                        self.resized_size.value += os.path.getsize(
                            out_filepath)

                os.remove(self.input_dir + os.path.sep + filename)
                logging.info("done resizing image {}".format(filename))
                self.img_queue.task_done()
            else:
                self.img_queue.task_done()
                break

        end = time.perf_counter()

        logging.info("created {} thumbnails in {} seconds".format(
            num_images, end - start))

    def make_thumbnails(self, img_url_list):
        logging.info("START make_thumbnails")
        dl_queue = Queue()
        # new
        dl_size_lock = multiprocessing.Lock()

        start = time.perf_counter()
        for _ in img_url_list:
            dl_queue.put(_)

        num_dl_threads = 4
        for _ in range(num_dl_threads):
            t = Thread(target=self.download_image,
                       args=(dl_queue, dl_size_lock))
            t.start()

        """
        two way to calculate the resized images size:
        => 
        child process communicate in queue with parent 
        whenever a resize operation completes
        and parent adds the size
        =>
        child process updates the shared var 
        whenever a resize operation completes 
        here this is adopted
        -->shared memory : good for single valie(adopted)
        -->process manager: more complex
        """
        num_processes = multiprocessing.cpu_count()
        for _ in range(num_processes):
            p = multiprocessing.Process(target=self.perform_resizing)
            p.start()

        dl_queue.join()
        for _ in range(num_processes):
            self.img_queue.put(None)

        end = time.perf_counter()
        logging.info("END make_thumbnails in {} seconds".format(end - start))
        logging.info("Initial size of downloads: [{}] Final size of images: [{}] ".format(
            self.dl_size, self.resized_size))
