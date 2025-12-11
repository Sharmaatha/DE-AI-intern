import axios from "axios";

const API = "http://localhost:8000";

export const getSignals = (date?: string) => {
  const url = date ? `${API}/signals?date=${date}` : `${API}/signals`;
  return axios.get(url);
};

export const checkDateExists = (date: string) =>
  axios.get(`${API}/signals/check-date/${date}`);

export const scrapeDate = (date: string) => {
  return axios.post(`${API}/scrape?sync=true`, { date: date, limit: 3 });
};
