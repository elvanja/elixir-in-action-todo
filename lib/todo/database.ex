defmodule Todo.Database do
  use GenServer

  @num_workers 3

  def start(db_folder) do
    GenServer.start(__MODULE__, db_folder, name: :database_server)
  end

  def store(key, data) do
    pick_worker(key)
    |> Todo.DatabaseWorker.store(key, data)
  end

  def get(key) do
    pick_worker(key)
    |> Todo.DatabaseWorker.get(key)
  end

  defp pick_worker(key) do
    GenServer.call(:database_server, {:pick_worker, key})
  end

  def init(db_folder) do
    {:ok, start_workers(db_folder)}
  end

  defp start_workers(db_folder) do
    (0..@num_workers - 1)
    |> Enum.map(fn(index) ->
      {:ok, pid} = Todo.DatabaseWorker.start(db_folder)
      {index, pid}
    end)
    |> Map.new
  end

  def handle_call({:pick_worker, key}, _, workers) do
    worker = :erlang.phash2(key, @num_workers)
    {:reply, Map.get(workers, worker), workers}
  end
end