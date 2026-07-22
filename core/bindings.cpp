#include "bounded_queue.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

namespace {

std::uint64_t parse_request_id(const py::handle request_id) {
    if (!PyLong_Check(request_id.ptr())) {
        throw py::type_error("request_id must be an integer");
    }

    const auto value = PyLong_AsUnsignedLongLong(request_id.ptr());
    if (PyErr_Occurred()) {
        throw py::error_already_set();
    }

    return static_cast<std::uint64_t>(value);
}

}  // namespace

PYBIND11_MODULE(miniserve_core, module) {
    module.doc() = "C++ bounded-queue primitives for miniserve-ai";

    py::class_<miniserve::BoundedQueue>(module, "BoundedQueue")
        .def(py::init<std::size_t>(), py::arg("capacity"))
        .def(
            "try_push",
            [](miniserve::BoundedQueue& queue, const py::handle request_id) {
                const auto parsed_request_id = parse_request_id(request_id);
                py::gil_scoped_release release;
                return queue.try_push(parsed_request_id);
            },
            py::arg("request_id"),
            "Atomically enqueue a request ID, or return False when full."
        )
        .def(
            "try_pop",
            &miniserve::BoundedQueue::try_pop,
            py::call_guard<py::gil_scoped_release>(),
            "Pop the oldest request ID, or return None when empty."
        )
        .def(
            "pop_batch",
            &miniserve::BoundedQueue::pop_batch,
            py::arg("max_items"),
            py::call_guard<py::gil_scoped_release>(),
            "Pop up to max_items request IDs in FIFO order."
        )
        .def_property_readonly(
            "size",
            [](const miniserve::BoundedQueue& queue) {
                py::gil_scoped_release release;
                return queue.size();
            }
        )
        .def_property_readonly("capacity", &miniserve::BoundedQueue::capacity)
        .def_property_readonly(
            "empty",
            [](const miniserve::BoundedQueue& queue) {
                py::gil_scoped_release release;
                return queue.empty();
            }
        )
        .def_property_readonly(
            "full",
            [](const miniserve::BoundedQueue& queue) {
                py::gil_scoped_release release;
                return queue.full();
            }
        );
}
