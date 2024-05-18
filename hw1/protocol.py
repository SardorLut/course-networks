import socket
import time
from queue import PriorityQueue


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

    def close(self):
        self.udp_socket.close()


def load(data: bytes, service_len: int):
    """
     Создает экземпляр `TCPSegment` из байтовой строки.

     :param data: Байтовая строка, содержащая номер последовательности, номер
                  подтверждения и данные сегмента.
     :return: Экземпляр `TCPSegment`.
     """
    seq = int.from_bytes(data[:8], "big", signed=False)
    ack = int.from_bytes(data[8:16], "big", signed=False)
    return TCPSegment(seq, ack, data[service_len:])


class TCPSegment:
    def __init__(self, seq_number: int, ack_number: int, data: bytes):
        self.service_len = 8 + 8
        self.ack_timeout = 0.05
        self.seq_number = seq_number
        self.ack_number = ack_number
        self.data = data
        self.acknowledged = False
        self._sending_time = time.time()

    def dump(self) -> bytes:
        """
        Возвращает объединенные байты номера последовательности, номера подтверждения
        и данных сегмента.

        :return: Байтовая строка, представляющая номер последовательности, номер
                 подтверждения и данные сегмента.
        """
        # Преобразует в 8-байтовое представление
        seq = self.seq_number.to_bytes(8, "big", signed=False)

        ack = self.ack_number.to_bytes(8, "big", signed=False)
        return seq + ack + self.data

    def update_sending_time(self, sending_time=None):
        """
        Обновляет время отправки сегмента.

        :param sending_time: Новое время отправки сегмента.
                             Если не указано, используется текущее время.
        """
        self._sending_time = sending_time if sending_time is not None else time.time()

    def expired(self):
        """
        был ли сегмент подтверждён, и превышает ли текущее время время отправки на большее значение,
        чем таймаут подтверждения (ack_timeout).
        :return: True, если сегмент устарел и не был подтвержден, иначе False.
        """
        return not self.acknowledged and (time.time() - self._sending_time > self.ack_timeout)

    def __len__(self):
        return len(self.data)


class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, name='Client', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        # Hyperparameters
        self.mss = 1000
        self.window_size = self.mss * 10
        self.read_timeout = 0.05
        self.ack_crit_lag = 10
        # Internal buffers
        self._sent_bytes_n = 0
        self._confirmed_bytes_n = 0
        self._received_bytes_n = 0
        self._send_window = PriorityQueue()
        self._recv_window = PriorityQueue()
        self._buffer = bytes()

    def send(self, data: bytes) -> int:
        sent_data_len = 0
        lag = 0
        while (data or self._confirmed_bytes_n < self._sent_bytes_n) and (lag < self.ack_crit_lag):
            if not self._is_window_locked() and data:
                sent_data_len += self._send_data_segment(data)
                data = data[self.mss:]

            if self._is_window_locked() or not data:
                lag = self._handle_ack_timeout(lag)
            else:
                self._receive_segment(0.0)
            self._resend_earliest_segment()
            print(f'{self.name} surely sent {self._confirmed_bytes_n}/{self._sent_bytes_n}.')

        self._log_final_status(lag)
        return sent_data_len

    def recv(self, n: int):
        print(f'{self.name} expects {n} bytes. ')
        right_border = min(n, len(self._buffer))
        data = self._buffer[:right_border]
        self._buffer = self._buffer[right_border:]
        while len(data) < n:
            self._receive_segment(self.read_timeout)
            right_border = min(n, len(self._buffer))
            data += self._buffer[:right_border]
            self._buffer = self._buffer[right_border:]
            print(f'{self.name} have read {len(data)} bytes, totally {self._received_bytes_n} bytes. ')
        print(f'{self.name} have read expected {n} bytes. ')
        return data

    def close(self):
        super().close()

    def _is_window_locked(self) -> bool:
        return self._sent_bytes_n - self._confirmed_bytes_n > self.window_size

    def _send_data_segment(self, data: bytes) -> int:
        right_border = min(self.mss, len(data))
        sent_length = self._send_segment(TCPSegment(self._sent_bytes_n, self._received_bytes_n, data[:right_border]))
        return sent_length

    def _handle_ack_timeout(self, lag: int) -> int:
        lst_recv_seg = self._receive_segment(0.05)
        if lst_recv_seg.seq_number == -1:
            print(f"{self.name} doesn't hear receiver!")
            lag += 1
        else:
            lag = 0
        return lag

    def _log_final_status(self, lag: int):
        if lag < self.ack_crit_lag:
            print(f'{self.name} surely sent all {self._sent_bytes_n} bytes.')
        else:
            print(f"{self.name} assumes that receiver is offline!")

    def _receive_segment(self, timeout: float = None) -> TCPSegment:
        self.udp_socket.settimeout(timeout)

        try:
            raw_data = self.recvfrom(self.mss + 16)
            segment = load(raw_data, 16)
        except socket.error:
            segment = TCPSegment(-1, -1, bytes())

        if len(segment):
            self._recv_window.put((segment.seq_number, segment))
            self._shift_recv_window()

        if segment.ack_number > self._confirmed_bytes_n:
            self._confirmed_bytes_n = segment.ack_number
            self._shift_send_window()

        return segment

    def _send_segment(self, segment: TCPSegment) -> int:
        self.udp_socket.settimeout(None)
        try:
            bytes_sent = self.sendto(segment.dump())
            just_sent = bytes_sent - segment.service_len

            if segment.seq_number == self._sent_bytes_n:
                self._sent_bytes_n += just_sent
            elif segment.seq_number > self._sent_bytes_n:
                raise ValueError(
                    f'Seq number {segment.seq_number} is incorrect, since it is bigger than the actual total length '
                    f'of sent messages: {self._sent_bytes_n}')

            segment.data = segment.data[:just_sent]

            if len(segment):
                segment.update_sending_time()
                self._send_window.put((segment.seq_number, segment))

            return just_sent

        except socket.error as e:
            raise RuntimeError(f'Error sending segment: {e}')

    def _shift_recv_window(self):
        earliest_segment = None
        while not self._recv_window.empty():
            _, earliest_segment = self._recv_window.get(block=False)
            if earliest_segment.seq_number < self._received_bytes_n:
                earliest_segment.acknowledged = True
            elif earliest_segment.seq_number == self._received_bytes_n:
                self._buffer += earliest_segment.data
                self._received_bytes_n += len(earliest_segment)
                earliest_segment.acknowledged = True
            else:
                self._recv_window.put((earliest_segment.seq_number, earliest_segment))
                break

        if earliest_segment is not None:
            self._send_segment(TCPSegment(self._sent_bytes_n, self._received_bytes_n, bytes()))

    def _shift_send_window(self):
        while not self._send_window.empty():
            _, earliest_segment = self._send_window.get(block=False)
            if earliest_segment.ack_number > self._confirmed_bytes_n:
                self._send_window.put((earliest_segment.seq_number, earliest_segment))
                break

    def _resend_earliest_segment(self, force=False):
        if self._send_window.empty():
            return
        _, earliest_segment = self._send_window.get(block=False)
        if earliest_segment.expired() or force:
            print(f'{self.name} sent {earliest_segment.seq_number} segment again! ')
            self._send_segment(earliest_segment)
        else:
            self._send_window.put((earliest_segment.seq_number, earliest_segment))
