import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 20 },
    { duration: '30s', target: 50 },
    { duration: '10s', target: 0 },
  ],
};

export default function () {
  const url = 'http://localhost:3000/jobs';
  const payload = JSON.stringify({
    taskName: 'send_welcome_email',
    data: { email: `user_${Math.floor(Math.random() * 100000)}@example.com` },
  });

  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  const res = http.post(url, payload, params);
  check(res, { 'status is 200 or 201': (r) => r.status === 200 || r.status === 201 });
  sleep(0.05);
}
