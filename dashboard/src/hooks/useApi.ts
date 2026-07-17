import axios from "axios";
import type { Job } from "../types";

const api = axios.create({ baseURL: "http://localhost:8000" });

export const useApi = () => ({
  getJobs: async (queue: string, status?: string, limit = 20): Promise<Job[]> => {
    const params = new URLSearchParams();
    if (status) params.append("status", status);
    params.append("limit", String(limit));
    const { data } = await api.get(`/queues/${queue}/jobs?${params}`);
    return data;
  },

  getJob: async (queue: string, id: string): Promise<Job> => {
    const { data } = await api.get(`/jobs/${queue}/${id}`);
    return data;
  },

  retryJob: async (queue: string, id: string): Promise<void> => {
    await api.post(`/jobs/${queue}/${id}/retry`);
  },

  retryAllFailed: async (queue: string): Promise<void> => {
    const jobs = await useApi().getJobs(queue, "failed", 100);
    await Promise.all(jobs.map((j) => api.post(`/jobs/${queue}/${j.id}/retry`)));
  },

  pauseQueue: async (queue: string): Promise<void> => {
    await api.post(`/queues/${queue}/pause`);
  },

  resumeQueue: async (queue: string): Promise<void> => {
    await api.post(`/queues/${queue}/resume`);
  },

  flushQueue: async (queue: string): Promise<void> => {
    await api.delete(`/queues/${queue}`);
  },
});